# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import os
import subprocess
from pathlib import Path

import pytest
import yaml

from ossie import OssieDocument
from ossie_dbt import cli
from ossie_dbt.msi_to_ossie import MSIToOssieConverter
from ossie_dbt.ossie_to_msi import OssieToMSIConverter
from metricflow_semantic_interfaces.implementations.element_config import (
    PydanticSemanticLayerElementConfig,
)
from metricflow_semantic_interfaces.test_utils import semantic_model_with_guaranteed_meta
from metricflow_semantic_interfaces.type_enums import AggregationType, DimensionType, EntityType, TimeGranularity
from tests.helpers import _dimension, _entity, _manifest, _measure, _simple_metric


def _dbt_meta(extensions: list | None) -> dict | None:
    for extension in extensions or []:
        if extension.vendor_name == "DBT":
            return json.loads(extension.data)
    return None


def test_config_meta_round_trips_for_all_supported_element_types() -> None:
    metadata = {
        "dataset": {"owner": "growth"},
        "entity": {"key_kind": "natural"},
        "dimension": {"display": {"format": "date"}},
        "measure": {"unit": "USD"},
        "metric": {"format": "currency"},
    }
    configs = {
        key: PydanticSemanticLayerElementConfig(meta=value)
        for key, value in metadata.items()
    }
    model = semantic_model_with_guaranteed_meta(
        name="orders",
        entities=[_entity("order_id", EntityType.PRIMARY).copy(update={"config": configs["entity"]})],
        dimensions=[
            _dimension("order_date", DimensionType.TIME, granularity=TimeGranularity.DAY).copy(
                update={"config": configs["dimension"]}
            )
        ],
        measures=[
            _measure("amount", AggregationType.SUM, expr="amount").copy(update={"config": configs["measure"]})
        ],
    ).copy(update={"config": configs["dataset"]})
    metric = _simple_metric("total_amount", "amount").copy(update={"config": configs["metric"]})

    document = MSIToOssieConverter().convert(_manifest([model], [metric])).output
    dataset = document.datasets[0]
    fields = {field.name: field for field in dataset.fields or []}
    ossie_metric = document.metrics[0]
    assert _dbt_meta(dataset.custom_extensions) == metadata["dataset"]
    assert _dbt_meta(fields["order_id"].custom_extensions) == metadata["entity"]
    assert _dbt_meta(fields["order_date"].custom_extensions) == metadata["dimension"]
    assert _dbt_meta(fields["amount"].custom_extensions) == metadata["measure"]
    assert _dbt_meta(ossie_metric.custom_extensions) == metadata["metric"]

    round_trip = OssieToMSIConverter().convert(document).output
    result_model = round_trip.semantic_models[0]
    assert result_model.config.meta == metadata["dataset"]
    assert result_model.entities[0].config.meta == metadata["entity"]
    result_dimensions = {dimension.name: dimension for dimension in result_model.dimensions}
    assert result_dimensions["order_date"].config.meta == metadata["dimension"]
    # Ossie fields are reconstructed as dimensions unless key metadata makes them entities.
    assert result_dimensions["amount"].config.meta == metadata["measure"]
    assert round_trip.metrics[0].config.meta == metadata["metric"]


def test_empty_or_missing_config_meta_is_omitted() -> None:
    empty_config = PydanticSemanticLayerElementConfig(meta={})
    model = semantic_model_with_guaranteed_meta(
        name="orders",
        entities=[_entity("order_id", EntityType.PRIMARY)],
        dimensions=[_dimension("status")],
        measures=[_measure("amount", AggregationType.SUM, expr="amount")],
    ).copy(update={"config": empty_config})
    metric = _simple_metric("total_amount", "amount")

    document = MSIToOssieConverter().convert(_manifest([model], [metric])).output
    dataset = document.datasets[0]
    assert dataset.custom_extensions is None
    assert document.metrics[0].custom_extensions is None
    assert all(field.custom_extensions is None for field in dataset.fields or [])


def test_invalid_dbt_custom_extension_data_is_rejected() -> None:
    raw = {
        "version": "0.2.0.dev0",
        "name": "test",
        "datasets": [
            {
                "name": "orders",
                "source": "schema.orders",
                "custom_extensions": [{"vendor_name": "DBT", "data": "not-json"}],
            }
        ],
    }
    with pytest.raises(ValueError, match="DBT custom extension data must contain a JSON object"):
        OssieToMSIConverter().convert(OssieDocument.model_validate(raw))


def test_duplicate_dbt_custom_extensions_are_rejected() -> None:
    extension = {"vendor_name": "DBT", "data": '{"owner":"growth"}'}
    raw = {
        "version": "0.2.0.dev0",
        "name": "test",
        "datasets": [
            {
                "name": "orders",
                "source": "schema.orders",
                "custom_extensions": [extension, extension],
            }
        ],
    }
    with pytest.raises(ValueError, match="Expected at most one DBT custom extension"):
        OssieToMSIConverter().convert(OssieDocument.model_validate(raw))


def test_real_dbt_parse_manifest_cli_and_round_trip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    project_dir = tmp_path / "dbt_project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)
    project_files = {
        "dbt_project.yml": """\
name: ossie_meta_e2e
version: '1.0.0'
config-version: 2
profile: ossie_meta_e2e
model-paths: [models]
""",
        "profiles.yml": """\
ossie_meta_e2e:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: target/ossie_meta_e2e.duckdb
      schema: main
      threads: 1
""",
        "models/orders.sql": "select 1 as order_id, 10 as amount, cast('2025-01-01' as date) as order_date\n",
        "models/time_spine.sql": (
            "select cast(date_day as date) as date_day from generate_series("
            "date '2025-01-01', date '2025-01-31', interval 1 day) as t(date_day)\n"
        ),
        "models/semantic_models.yml": """\
version: 2
semantic_models:
  - name: orders
    model: ref('orders')
    defaults:
      agg_time_dimension: order_date
    config:
      meta:
        owner: growth
    entities:
      - name: order_id
        type: primary
        expr: order_id
        config:
          meta:
            key_kind: natural
    dimensions:
      - name: order_date
        type: time
        type_params:
          time_granularity: day
        config:
          meta:
            display: date
    measures:
      - name: total_amount
        agg: sum
        expr: amount
        config:
          meta:
            unit: USD
metrics:
  - name: total_revenue
    type: simple
    type_params:
      measure: total_amount
    config:
      meta:
        value_format: currency
models:
  - name: time_spine
    time_spine:
      standard_granularity_column: date_day
    columns:
      - name: date_day
        granularity: day
""",
    }
    for relative_path, contents in project_files.items():
        (project_dir / relative_path).write_text(contents)

    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(project_dir)
    version = subprocess.run(["dbt", "--version"], check=True, capture_output=True, text=True)
    assert "Core:" in version.stdout, "The E2E requires dbt Core, not the dbt Cloud CLI"
    subprocess.run(
        ["dbt", "parse", "--no-partial-parse"],
        cwd=project_dir,
        env=env,
        check=True,
        capture_output=True,
        text=True,
    )

    manifest_path = project_dir / "target" / "semantic_manifest.json"
    assert manifest_path.is_file()
    output_path = project_dir / "target" / "ossie.yaml"
    monkeypatch.setattr(
        "sys.argv",
        ["ossie-dbt", "msi-to-ossie", "-i", str(manifest_path), "-o", str(output_path)],
    )
    cli.main()

    document = OssieDocument.model_validate(yaml.safe_load(output_path.read_text()))
    dataset = document.datasets[0]
    fields = {field.name: field for field in dataset.fields or []}
    assert _dbt_meta(dataset.custom_extensions) == {"owner": "growth"}
    assert _dbt_meta(fields["order_id"].custom_extensions) == {"owner": "growth", "key_kind": "natural"}
    assert _dbt_meta(fields["order_date"].custom_extensions) == {"owner": "growth", "display": "date"}
    assert _dbt_meta(fields["total_amount"].custom_extensions) == {"owner": "growth", "unit": "USD"}
    assert _dbt_meta(document.metrics[0].custom_extensions) == {"value_format": "currency"}

    round_trip = OssieToMSIConverter().convert(document).output
    result_model = round_trip.semantic_models[0]
    assert result_model.config.meta == {"owner": "growth"}
    assert result_model.entities[0].config.meta == {"owner": "growth", "key_kind": "natural"}
    dimensions = {dimension.name: dimension for dimension in result_model.dimensions}
    assert dimensions["order_date"].config.meta == {"owner": "growth", "display": "date"}
    assert dimensions["total_amount"].config.meta == {"owner": "growth", "unit": "USD"}
    assert round_trip.metrics[0].config.meta == {"value_format": "currency"}
