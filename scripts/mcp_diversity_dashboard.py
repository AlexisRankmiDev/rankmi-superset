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

"""
Create the **Dashboard de diversidad** (D&I) via Superset MCP.

Source table in Redshift: ``master_dwh.user_details`` (same grain as the
``master_dwh.users_Details`` naming used in specs; identifiers are lowercased
in ``information_schema``).

Base filters are embedded in the virtual dataset SQL. Add **native dashboard
filters** in the Superset UI for ``enterprise_token``, ``area_name``,
``position_level_name``, and ``country_es`` (MCP ``generate_dashboard`` does
not yet wire ``native_filter_configuration``).

Run::

    export MCP_URL=http://127.0.0.1:5008/mcp
    python scripts/mcp_diversity_dashboard.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any

MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:5008/mcp")

# SQL applied to all charts on this dataset.
DIVERSITY_MASTER_SQL = """
SELECT
  user_id,
  user_token,
  enterprise_token,
  enterprise_name,
  gender_es,
  position_level_name,
  area_name,
  generation_category,
  age_category,
  CAST(age_category_order AS BIGINT) AS age_category_sort,
  (LPAD(CAST(age_category_order AS VARCHAR(10)), 4, '0') || ' — ' ||
   COALESCE(CAST(age_category AS VARCHAR(512)), 'Sin dato')) AS age_category_label,
  nationality_es,
  civil_status,
  country_es,
  CAST(age AS DOUBLE PRECISION) AS age,
  CAST(COALESCE(seniority_years, tenure) AS DOUBLE PRECISION) AS tenure_years
FROM master_dwh.user_details
WHERE is_active IS TRUE
  AND admin_role IS NULL
""".strip()


def _log(msg: str) -> None:
    print(msg, flush=True)


async def _call(client: Any, tool: str, payload: dict[str, Any]) -> dict[str, Any]:
    r = await client.call_tool(tool, payload)
    if not r.content:
        raise RuntimeError(f"empty MCP response for {tool}")
    text = r.content[0].text
    if not text.strip():
        raise RuntimeError(f"empty text for {tool}")
    return json.loads(text)


async def _gen_chart(client: Any, ds_id: int, cfg: dict[str, Any], name: str) -> int:
    out = await _call(
        client,
        "generate_chart",
        {
            "request": {
                "dataset_id": ds_id,
                "config": cfg,
                "chart_name": name,
                "save_chart": True,
                "generate_preview": False,
            }
        },
    )
    if out.get("error") or not (out.get("chart") or {}).get("id"):
        raise RuntimeError(json.dumps(out, default=str)[:4000])
    return int(out["chart"]["id"])


async def main() -> int:
    try:
        from fastmcp import Client
    except ImportError:
        _log("Install fastmcp: pip install fastmcp")
        return 1

    async with Client(MCP_URL) as client:
        dbs = await _call(client, "list_databases", {"request": {"page": 1, "page_size": 50}})
        redshift = next(
            (d for d in dbs.get("databases", []) if d.get("backend") == "redshift"), None
        )
        if not redshift:
            _log("No Redshift database in list_databases.")
            return 1
        db_id = int(redshift["id"])

        dry = await _call(
            client,
            "execute_sql",
            {
                "request": {
                    "database_id": db_id,
                    "sql": DIVERSITY_MASTER_SQL,
                    "schema": "master_dwh",
                    "limit": 5,
                    "timeout": 180,
                }
            },
        )
        if not dry.get("success"):
            _log(dry.get("error", str(dry)))
            return 1
        _log("Dry-run OK.")

        ds_name = "mcp_dwh_diversity_users_details"
        created = await _call(
            client,
            "create_virtual_dataset",
            {
                "request": {
                    "database_id": db_id,
                    "sql": DIVERSITY_MASTER_SQL,
                    "dataset_name": ds_name,
                    "schema": "master_dwh",
                    "description": (
                        "D&I: activos, admin_role nulo; filtrar en dashboard por "
                        "enterprise_token, area_name, position_level_name, country_es."
                    ),
                }
            },
        )
        if created.get("error") or not created.get("id"):
            _log(f"create_virtual_dataset failed: {created}")
            return 1
        ds_id = int(created["id"])
        _log(f"Virtual dataset id={ds_id}")

        charts: list[int] = []
        charts.append(
            await _gen_chart(
                client,
                ds_id,
                {
                    "chart_type": "big_number",
                    "metric": {"name": "user_id", "aggregate": "COUNT_DISTINCT"},
                    "show_trendline": False,
                    "subheader": "Headcount activo (sin cuentas admin)",
                },
                "D&I — Headcount",
            )
        )
        charts.append(
            await _gen_chart(
                client,
                ds_id,
                {
                    "chart_type": "big_number",
                    "metric": {"name": "age", "aggregate": "AVG"},
                    "show_trendline": False,
                    "subheader": "Años",
                    "y_axis_format": ",.1f",
                },
                "D&I — Edad promedio",
            )
        )
        charts.append(
            await _gen_chart(
                client,
                ds_id,
                {
                    "chart_type": "big_number",
                    "metric": {"name": "tenure_years", "aggregate": "AVG"},
                    "show_trendline": False,
                    "subheader": "Años (seniority o tenure)",
                    "y_axis_format": ",.1f",
                },
                "D&I — Tenure promedio",
            )
        )
        charts.append(
            await _gen_chart(
                client,
                ds_id,
                {
                    "chart_type": "pie",
                    "dimension": {"name": "gender_es"},
                    "metric": {"name": "user_id", "aggregate": "COUNT_DISTINCT"},
                    "donut": True,
                    "show_total": True,
                    "row_limit": 50,
                },
                "D&I — Género (donut)",
            )
        )
        charts.append(
            await _gen_chart(
                client,
                ds_id,
                {
                    "chart_type": "xy",
                    "x": {"name": "position_level_name"},
                    "y": [
                        {
                            "name": "user_id",
                            "aggregate": "COUNT_DISTINCT",
                            "label": "Colaboradores",
                        }
                    ],
                    "group_by": [{"name": "gender_es"}],
                    "kind": "bar",
                    "stacked": True,
                    "orientation": "horizontal",
                    "row_limit": 5000,
                },
                "D&I — Género × nivel jerárquico",
            )
        )
        charts.append(
            await _gen_chart(
                client,
                ds_id,
                {
                    "chart_type": "xy",
                    "x": {"name": "area_name"},
                    "y": [
                        {
                            "name": "user_id",
                            "aggregate": "COUNT_DISTINCT",
                            "label": "Colaboradores",
                        }
                    ],
                    "group_by": [{"name": "gender_es"}],
                    "kind": "bar",
                    "stacked": True,
                    "orientation": "horizontal",
                    "row_limit": 5000,
                },
                "D&I — Género × área",
            )
        )
        charts.append(
            await _gen_chart(
                client,
                ds_id,
                {
                    "chart_type": "pie",
                    "dimension": {"name": "generation_category"},
                    "metric": {"name": "user_id", "aggregate": "COUNT_DISTINCT"},
                    "donut": True,
                    "row_limit": 50,
                },
                "D&I — Generación",
            )
        )
        charts.append(
            await _gen_chart(
                client,
                ds_id,
                {
                    "chart_type": "xy",
                    "x": {"name": "age_category_label"},
                    "y": [{"name": "user_id", "aggregate": "COUNT_DISTINCT"}],
                    "kind": "bar",
                    "orientation": "horizontal",
                    "row_limit": 100,
                },
                "D&I — Pirámide de edad",
            )
        )
        charts.append(
            await _gen_chart(
                client,
                ds_id,
                {
                    "chart_type": "big_number",
                    "metric": {"name": "nationality_es", "aggregate": "COUNT_DISTINCT"},
                    "show_trendline": False,
                    "subheader": "Nacionalidades distintas",
                },
                "D&I — Nacionalidades (conteo)",
            )
        )
        charts.append(
            await _gen_chart(
                client,
                ds_id,
                {
                    "chart_type": "xy",
                    "x": {"name": "nationality_es"},
                    "y": [{"name": "user_id", "aggregate": "COUNT_DISTINCT"}],
                    "kind": "bar",
                    "orientation": "horizontal",
                    "row_limit": 40,
                },
                "D&I — Distribución nacionalidad",
            )
        )
        charts.append(
            await _gen_chart(
                client,
                ds_id,
                {
                    "chart_type": "pie",
                    "dimension": {"name": "civil_status"},
                    "metric": {"name": "user_id", "aggregate": "COUNT_DISTINCT"},
                    "donut": False,
                    "row_limit": 30,
                },
                "D&I — Estado civil",
            )
        )

        dash = await _call(
            client,
            "generate_dashboard",
            {
                "request": {
                    "chart_ids": charts,
                    "dashboard_title": "Dashboard de diversidad",
                    "description": (
                        "People Analytics D&I sobre master_dwh.user_details "
                        "(activos, admin_role nulo). Añade filtros nativos: "
                        "enterprise_token, area_name, position_level_name, country_es."
                    ),
                    "published": False,
                }
            },
        )
        if dash.get("error"):
            _log(str(dash))
            return 1
        _log(json.dumps(dash, indent=2, default=str)[:5000])
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
