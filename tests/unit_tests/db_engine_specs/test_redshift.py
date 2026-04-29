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
from sqlalchemy.engine.url import make_url

from superset.db_engine_specs.redshift import RedshiftEngineSpec


def test_adjust_engine_params_loopback_uses_require_without_hostname_check() -> None:
    url = make_url(
        "redshift+psycopg2://u:p@127.0.0.1:15439/dev"
        "?sslmode=verify-ca&connect_timeout=10"
    )
    out_url, connect_args = RedshiftEngineSpec.adjust_engine_params(url, {})
    assert "sslmode" not in (out_url.query or {})
    assert connect_args.get("sslmode") == "require"
    assert "connect_timeout" in (out_url.query or {})


def test_adjust_engine_params_non_loopback_leaves_url_query() -> None:
    url = make_url(
        "redshift+psycopg2://u:p@mycluster.abc123.us-east-1"
        ".redshift.amazonaws.com:5439/prod?sslmode=verify-ca"
    )
    out_url, connect_args = RedshiftEngineSpec.adjust_engine_params(url, {})
    assert (out_url.query or {}).get("sslmode") == "verify-ca"
    assert "sslmode" not in connect_args


def test_adjust_engine_params_loopback_applies_after_catalog() -> None:
    url = make_url("redshift+psycopg2://u:p@127.0.0.1:15439/dev?sslmode=verify-ca")
    out_url, _ = RedshiftEngineSpec.adjust_engine_params(
        url, {}, catalog="otherdb", schema=None
    )
    assert out_url.database == "otherdb"
    assert "sslmode" not in (out_url.query or {})
