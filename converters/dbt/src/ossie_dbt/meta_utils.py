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
from typing import Optional, Sequence

from ossie import OssieCustomExtension
from metricflow_semantic_interfaces.implementations.element_config import (
    PydanticSemanticLayerElementConfig,
)


def _meta_to_custom_extensions(
    config: Optional[PydanticSemanticLayerElementConfig],
) -> Optional[list[OssieCustomExtension]]:
    """Serialize non-empty dbt ``config.meta`` into Ossie's DBT extension."""
    meta = config.meta if config is not None else None
    if not meta:
        return None
    return [
        OssieCustomExtension(
            vendor_name="DBT",
            data=json.dumps(meta, ensure_ascii=False, sort_keys=True),
        )
    ]


def _meta_config_from_custom_extensions(
    extensions: Optional[Sequence[OssieCustomExtension]],
) -> Optional[PydanticSemanticLayerElementConfig]:
    """Restore dbt ``config.meta`` from an Ossie DBT custom extension."""
    dbt_extensions = [extension for extension in extensions or [] if extension.vendor_name == "DBT"]
    if not dbt_extensions:
        return None
    if len(dbt_extensions) > 1:
        raise ValueError("Expected at most one DBT custom extension for config.meta")

    try:
        meta = json.loads(dbt_extensions[0].data)
    except json.JSONDecodeError as exc:
        raise ValueError("DBT custom extension data must contain a JSON object for config.meta") from exc
    if not isinstance(meta, dict):
        raise ValueError("DBT custom extension data must contain a JSON object for config.meta")
    return PydanticSemanticLayerElementConfig(meta=meta)
