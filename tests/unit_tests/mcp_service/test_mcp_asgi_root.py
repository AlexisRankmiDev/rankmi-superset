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

from __future__ import annotations

from starlette.responses import PlainTextResponse
from starlette.testclient import TestClient

from superset.mcp_service.server import _wrap_mcp_asgi_with_root_info


def test_wrap_mcp_asgi_serves_info_on_get_slash() -> None:
    """GET / returns JSON with mcp URL; other paths go to the inner ASGI app."""

    async def inner_asgi(scope, receive, send) -> None:  # noqa: ANN001
        if scope.get("type") == "http":
            await PlainTextResponse("inner-ok", status_code=200)(scope, receive, send)

    wrapped = _wrap_mcp_asgi_with_root_info(inner_asgi, "127.0.0.1", 5008)
    client = TestClient(wrapped)
    r = client.get("/")
    assert r.status_code == 200
    data = r.json()
    assert data["service"] == "Apache Superset MCP"
    assert data["mcp"] == "http://127.0.0.1:5008/mcp"
    r2 = client.get("/mcp")
    assert r2.status_code == 200
    data2 = r2.json()
    assert data2["service"] == "Apache Superset MCP"
    assert data2["endpoint"] == "http://127.0.0.1:5008/mcp"
    assert "note" in data2
    r3 = client.get("/other")
    assert r3.status_code == 200
    assert r3.text == "inner-ok"


def test_wrap_mcp_asgi_get_mcp_uses_127_for_bind_all_host() -> None:
    """When server binds 0.0.0.0, discovery JSON uses 127.0.0.1 for humans."""

    async def inner_asgi(scope, receive, send) -> None:  # noqa: ANN001
        if scope.get("type") == "http":
            await PlainTextResponse("x")(scope, receive, send)

    wrapped = _wrap_mcp_asgi_with_root_info(inner_asgi, "0.0.0.0", 5008)
    client = TestClient(wrapped)
    assert client.get("/").json()["mcp"] == "http://127.0.0.1:5008/mcp"
    assert client.get("/mcp").json()["endpoint"] == "http://127.0.0.1:5008/mcp"


def test_wrap_mcp_asgi_favicon_no_content() -> None:
    async def inner_asgi(scope, receive, send) -> None:  # noqa: ANN001
        if scope.get("type") == "http":
            await PlainTextResponse("no")(scope, receive, send)

    wrapped = _wrap_mcp_asgi_with_root_info(inner_asgi, "127.0.0.1", 1)
    client = TestClient(wrapped)
    r = client.get("/favicon.ico")
    assert r.status_code == 204
    assert r.content == b""
