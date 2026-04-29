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
Create a Superset dashboard (via MCP) for gender diversity KPIs in schema ``master``.

Run from the repo root with MCP up (e.g. Docker ``superset-mcp`` on port 5008)::

    source .venv/bin/activate
    export MCP_URL=http://127.0.0.1:5008/mcp
    python scripts/mcp_master_gender_diversity_dashboard.py

The script discovers ``master.users`` gender columns and a join key to
``master.enterprises`` (``enterprise_id`` / ``company_id`` / ``monolith_id`` /
``database_name``), then builds virtual datasets and charts. Geography uses
``users.country`` (or similar) when present, not branch joins, to avoid duplicate rows.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from typing import Any

MCP_URL = os.environ.get("MCP_URL", "http://127.0.0.1:5008/mcp")


def _j(msg: str) -> None:
    print(msg, flush=True)


async def _call(client: Any, tool: str, payload: dict[str, Any]) -> dict[str, Any]:
    r = await client.call_tool(tool, payload)
    if not r.content:
        raise RuntimeError(f"empty MCP response for {tool}")
    text = r.content[0].text
    if not text.strip():
        raise RuntimeError(f"empty text for {tool}")
    return json.loads(text)


async def _execute_sql(client: Any, database_id: int, sql: str, **kw: Any) -> dict[str, Any]:
    req: dict[str, Any] = {"database_id": database_id, "sql": sql, "schema": "master"}
    req.update(kw)
    return await _call(client, "execute_sql", {"request": req})


async def _columns(
    client: Any, database_id: int, table: str
) -> list[dict[str, Any]]:
    sql = f"""
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'master' AND table_name = '{table}'
    ORDER BY ordinal_position
    """
    out = await _execute_sql(client, database_id, sql, limit=500, timeout=120)
    if not out.get("success"):
        raise RuntimeError(out.get("error") or "column query failed")
    return list(out.get("rows") or [])


def _pick_gender_column(cols: list[dict[str, Any]]) -> str | None:
    names = {c["column_name"].lower(): c["column_name"] for c in cols}
    for candidate in (
        "gender",
        "sex",
        "sexo",
        "genero",
        "gender_id",
        "sex_id",
    ):
        if candidate in names:
            return names[candidate]
    return None


def _pick_enterprise_join(cols: list[dict[str, Any]]) -> tuple[str, str] | None:
    """Return (users_column, enterprises_column) for JOIN."""
    names = {c["column_name"].lower(): c["column_name"] for c in cols}
    if "enterprise_id" in names:
        return names["enterprise_id"], "id"
    if "company_id" in names:
        return names["company_id"], "id"
    if "monolith_id" in names:
        return names["monolith_id"], "monolith_id"
    # Multi-tenant catalog: tenant DB name matches enterprise row (Rankmi / monolith).
    if "database_name" in names:
        return names["database_name"], "database_name"
    return None


def _pick_geo_columns(branch_cols: list[dict[str, Any]]) -> str | None:
    """Return a single expression label for geography, or None."""
    n = {c["column_name"].lower(): c["column_name"] for c in branch_cols}
    for key in ("country", "country_code", "pais", "region", "city", "ciudad"):
        if key in n:
            return n[key]
    return None


def _pick_user_geo_column(user_cols: list[dict[str, Any]]) -> str | None:
    """Geography on the user row (avoids fan-out from joining all branches)."""
    n = {c["column_name"].lower(): c["column_name"] for c in user_cols}
    for key in ("country", "nationality", "region", "state", "city", "ciudad"):
        if key in n:
            return n[key]
    return None


def _build_kpi_sql(
    gender_col: str,
    u_ent_col: str,
    e_ent_col: str,
    user_geo_col: str | None,
) -> str:
    """Build KPI query: gender by enterprise; geography from the user row (no branch fan-out)."""
    if user_geo_col:
        geo_select = f"COALESCE(CAST(u.{user_geo_col} AS VARCHAR(512)), 'Sin dato') AS geo_label"
    else:
        geo_select = "'Sin dato geográfico' AS geo_label"

    return f"""
WITH raw AS (
  SELECT
    e.id AS enterprise_id,
    e.name AS enterprise_name,
    u.{gender_col} AS gender_raw,
    {geo_select}
  FROM master.enterprises e
  INNER JOIN master.users u
    ON u.{u_ent_col} = e.{e_ent_col}
),
norm AS (
  SELECT
    enterprise_id,
    enterprise_name,
    geo_label,
    CASE
      WHEN LOWER(TRIM(CAST(gender_raw AS VARCHAR(256)))) IN (
        'm','male','masculino','hombre','1','h','man'
      ) THEN 'male'
      WHEN LOWER(TRIM(CAST(gender_raw AS VARCHAR(256)))) IN (
        'f','female','femenino','mujer','2','w','woman'
      ) THEN 'female'
      ELSE 'other'
    END AS gender
  FROM raw
  WHERE gender_raw IS NOT NULL
),
by_emp AS (
  SELECT
    enterprise_id,
    enterprise_name,
    geo_label,
    SUM(CASE WHEN gender = 'male' THEN 1 ELSE 0 END) AS cnt_male,
    SUM(CASE WHEN gender = 'female' THEN 1 ELSE 0 END) AS cnt_female,
    SUM(CASE WHEN gender = 'other' THEN 1 ELSE 0 END) AS cnt_other,
    COUNT(*) AS cnt_total
  FROM norm
  GROUP BY 1, 2, 3
)
SELECT
  enterprise_id,
  enterprise_name,
  geo_label,
  cnt_male,
  cnt_female,
  cnt_other,
  cnt_total,
  CAST(
    CASE WHEN cnt_total > 0 THEN 1.0 * cnt_female / cnt_total END AS DOUBLE PRECISION
  ) AS pct_female,
  CAST(
    CASE WHEN cnt_total > 0 THEN 1.0 * cnt_male / cnt_total END AS DOUBLE PRECISION
  ) AS pct_male,
  CAST(
    CASE WHEN cnt_total > 0 THEN
      -1.0 * (
        CASE WHEN cnt_male > 0
          THEN (cnt_male::FLOAT / cnt_total) * LN(cnt_male::FLOAT / cnt_total) ELSE 0 END +
        CASE WHEN cnt_female > 0
          THEN (cnt_female::FLOAT / cnt_total) * LN(cnt_female::FLOAT / cnt_total) ELSE 0 END +
        CASE WHEN cnt_other > 0
          THEN (cnt_other::FLOAT / cnt_total) * LN(cnt_other::FLOAT / cnt_total) ELSE 0 END
      )
    END AS DOUBLE PRECISION
  ) AS gender_diversity_shannon
FROM by_emp
WHERE cnt_total > 0
ORDER BY cnt_total DESC
""".strip()


async def main() -> int:
    try:
        from fastmcp import Client
    except ImportError:
        _j("Install fastmcp in this environment: pip install fastmcp")
        return 1

    async with Client(MCP_URL) as client:
        dbs = await _call(client, "list_databases", {"request": {"page": 1, "page_size": 50}})
        redshift = next(
            (d for d in dbs.get("databases", []) if d.get("backend") == "redshift"), None
        )
        if not redshift:
            _j("No Redshift database found in list_databases.")
            return 1
        db_id = int(redshift["id"])
        _j(f"Using database_id={db_id} ({redshift.get('database_name')})")

        ucols = await _columns(client, db_id, "users")
        bcols: list[dict[str, Any]] = []
        try:
            bcols = await _columns(client, db_id, "enterprises_branches")
        except RuntimeError:
            _j("No enterprises_branches metadata (geo join skipped).")

        gender = _pick_gender_column(ucols)
        join = _pick_enterprise_join(ucols)
        user_geo_col = _pick_user_geo_column(ucols)
        branch_geo_col = _pick_geo_columns(bcols)

        _j(f"Detected gender column: {gender}")
        _j(f"Detected enterprise join: {join}")
        _j(f"User-level geo column: {user_geo_col}")
        if branch_geo_col:
            _j(f"(enterprises_branches has {branch_geo_col}; not used to avoid row fan-out)")

        if not gender:
            _j("Could not find a gender column on master.users. Columns:")
            for c in ucols[:40]:
                _j(f"  {c.get('column_name')} {c.get('data_type')}")
            return 1
        if not join:
            _j("Could not infer join users -> enterprises. user columns sample:")
            for c in ucols[:30]:
                _j(f"  {c.get('column_name')}")
            return 1

        u_ent_col, e_ent_col = join
        kpi_sql = _build_kpi_sql(gender, u_ent_col, e_ent_col, user_geo_col)

        dry = await _execute_sql(client, db_id, kpi_sql, limit=20, timeout=180)
        if not dry.get("success"):
            _j("KPI SQL dry-run failed:")
            _j(dry.get("error", str(dry)))
            return 1
        _j(f"Dry-run OK, sample rows: {dry.get('row_count')}")

        ds_name = "mcp_master_kpi_gender_diversity_geo"
        created = await _call(
            client,
            "create_virtual_dataset",
            {
                "request": {
                    "database_id": db_id,
                    "sql": kpi_sql,
                    "dataset_name": ds_name,
                    "schema": "master",
                    "description": "MCP: diversidad de género por empresa y etiqueta geográfica (heurística).",
                }
            },
        )
        if created.get("error") or not created.get("id"):
            _j(f"create_virtual_dataset failed: {created}")
            return 1
        ds_id = int(created["id"])
        _j(f"Virtual dataset id={ds_id}")

        charts: list[int] = []

        async def gen(cfg: dict[str, Any], chart_name: str) -> int:
            out = await _call(
                client,
                "generate_chart",
                {
                    "request": {
                        "dataset_id": ds_id,
                        "config": cfg,
                        "chart_name": chart_name,
                        "save_chart": True,
                        "generate_preview": False,
                    }
                },
            )
            if out.get("error") or not (out.get("chart") or {}).get("id"):
                raise RuntimeError(str(out))
            return int(out["chart"]["id"])

        # Stacked bar: enterprise x gender counts (wide columns)
        charts.append(
            await gen(
                {
                    "chart_type": "xy",
                    "x": {"name": "enterprise_name"},
                    "y": [
                        {"name": "cnt_male", "aggregate": "SUM"},
                        {"name": "cnt_female", "aggregate": "SUM"},
                        {"name": "cnt_other", "aggregate": "SUM"},
                    ],
                    "kind": "bar",
                    "stacked": True,
                    "orientation": "horizontal",
                    "row_limit": 5000,
                },
                "MCP master — Género por empresa (apilado)",
            )
        )

        charts.append(
            await gen(
                {
                    "chart_type": "xy",
                    "x": {"name": "geo_label"},
                    "y": [
                        {"name": "cnt_male", "aggregate": "SUM"},
                        {"name": "cnt_female", "aggregate": "SUM"},
                    ],
                    "kind": "bar",
                    "stacked": True,
                    "row_limit": 200,
                },
                "MCP master — Género por etiqueta geográfica",
            )
        )

        charts.append(
            await gen(
                {
                    "chart_type": "table",
                    "columns": [
                        {"name": "enterprise_name", "label": "Empresa"},
                        {"name": "geo_label", "label": "Geo"},
                        {"name": "cnt_total", "label": "Personas", "aggregate": "SUM"},
                        {"name": "pct_female", "label": "% mujeres", "aggregate": "AVG"},
                        {"name": "gender_diversity_shannon", "label": "Diversidad (Shannon)", "aggregate": "AVG"},
                    ],
                    "row_limit": 500,
                },
                "MCP master — Tabla KPI diversidad",
            )
        )

        dash = await _call(
            client,
            "generate_dashboard",
            {
                "request": {
                    "chart_ids": charts,
                    "dashboard_title": "MCP — Diversidad de género (master)",
                    "description": "KPIs por empresa y distribución geográfica (dataset virtual generado por script).",
                    "published": False,
                }
            },
        )
        if dash.get("error"):
            _j(str(dash))
            return 1
        _j(json.dumps(dash, indent=2, default=str)[:4000])
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(main()))
    except KeyboardInterrupt:
        sys.exit(130)
