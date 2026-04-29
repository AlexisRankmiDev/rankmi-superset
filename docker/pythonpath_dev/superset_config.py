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
#
# This file is included in the final Docker image and SHOULD be overridden when
# deploying the image to prod. Settings configured here are intended for use in local
# development environments. Also note that superset_config_docker.py is imported
# as a final step as a means to override "defaults" configured here
#
import logging
import os
import sys
from typing import Any

from celery.schedules import crontab
from flask_caching.backends.filesystemcache import FileSystemCache
from sqlalchemy.engine.url import URL

logger = logging.getLogger()

DATABASE_DIALECT = os.getenv("DATABASE_DIALECT")
DATABASE_USER = os.getenv("DATABASE_USER")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD")
DATABASE_HOST = os.getenv("DATABASE_HOST")
DATABASE_PORT = os.getenv("DATABASE_PORT")
DATABASE_DB = os.getenv("DATABASE_DB")

EXAMPLES_USER = os.getenv("EXAMPLES_USER")
EXAMPLES_PASSWORD = os.getenv("EXAMPLES_PASSWORD")
EXAMPLES_HOST = os.getenv("EXAMPLES_HOST")
EXAMPLES_PORT = os.getenv("EXAMPLES_PORT")
EXAMPLES_DB = os.getenv("EXAMPLES_DB")

# The SQLAlchemy connection string.
SQLALCHEMY_DATABASE_URI = (
    f"{DATABASE_DIALECT}://"
    f"{DATABASE_USER}:{DATABASE_PASSWORD}@"
    f"{DATABASE_HOST}:{DATABASE_PORT}/{DATABASE_DB}"
)

# Use environment variable if set, otherwise construct from components
# This MUST take precedence over any other configuration
SQLALCHEMY_EXAMPLES_URI = os.getenv(
    "SUPERSET__SQLALCHEMY_EXAMPLES_URI",
    (
        f"{DATABASE_DIALECT}://"
        f"{EXAMPLES_USER}:{EXAMPLES_PASSWORD}@"
        f"{EXAMPLES_HOST}:{EXAMPLES_PORT}/{EXAMPLES_DB}"
    ),
)


REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = os.getenv("REDIS_PORT", "6379")
REDIS_CELERY_DB = os.getenv("REDIS_CELERY_DB", "0")
REDIS_RESULTS_DB = os.getenv("REDIS_RESULTS_DB", "1")

RESULTS_BACKEND = FileSystemCache("/app/superset_home/sqllab")

CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 300,
    "CACHE_KEY_PREFIX": "superset_",
    "CACHE_REDIS_HOST": REDIS_HOST,
    "CACHE_REDIS_PORT": REDIS_PORT,
    "CACHE_REDIS_DB": REDIS_RESULTS_DB,
}
DATA_CACHE_CONFIG = CACHE_CONFIG
THUMBNAIL_CACHE_CONFIG = CACHE_CONFIG


class CeleryConfig:
    broker_url = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}"
    imports = (
        "superset.sql_lab",
        "superset.tasks.scheduler",
        "superset.tasks.thumbnails",
        "superset.tasks.cache",
    )
    result_backend = f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_RESULTS_DB}"
    worker_prefetch_multiplier = 1
    task_acks_late = False
    beat_schedule = {
        "reports.scheduler": {
            "task": "reports.scheduler",
            "schedule": crontab(minute="*", hour="*"),
        },
        "reports.prune_log": {
            "task": "reports.prune_log",
            "schedule": crontab(minute=10, hour=0),
        },
    }


CELERY_CONFIG = CeleryConfig

FEATURE_FLAGS = {"ALERT_REPORTS": True, "DATASET_FOLDERS": True}
ALERT_REPORTS_NOTIFICATION_DRY_RUN = True

# MCP (Model Context Protocol): used by the optional superset-mcp Docker service and
# local `superset mcp run`. AI clients call this HTTP endpoint; they do not send OpenAI
# API keys to Superset. For production use JWT/API keys and set MCP_DEV_USERNAME to None.
MCP_DEV_USERNAME = os.getenv("MCP_DEV_USERNAME", "admin")

if os.getenv("DATABASE_HOST") == "db":
    _superset_internal_port = os.getenv("SUPERSET_PORT", "8088")
    # Match docker-compose `${SUPERSET_PORT:-8088}:8088` host port for browser links
    # (MCP uses get_superset_base_url(); old config hardcoded 8888). Use
    # SUPERSET_PUBLISHED_HOST_PORT if the UI is reached via another port (e.g. nginx on 80).
    _superset_browser_port = os.getenv("SUPERSET_PUBLISHED_HOST_PORT", _superset_internal_port)
    WEBDRIVER_BASEURL = (
        f"http://superset:{_superset_internal_port}"
        f"{os.environ.get('SUPERSET_APP_ROOT', '/')}/"
    )
    WEBDRIVER_BASEURL_USER_FRIENDLY = (
        f"http://localhost:{_superset_browser_port}/{os.environ.get('SUPERSET_APP_ROOT', '/')}/"
    )
    SUPERSET_WEBSERVER_ADDRESS = os.getenv(
        "SUPERSET_WEBSERVER_ADDRESS",
        f"http://superset:{_superset_internal_port}",
    )
else:
    _superset_browser_port_else = os.getenv(
        "SUPERSET_PUBLISHED_HOST_PORT",
        os.getenv("SUPERSET_PORT", "8088"),
    )
    WEBDRIVER_BASEURL = f"http://superset_app{os.environ.get('SUPERSET_APP_ROOT', '/')}/"  # noqa: E501
    WEBDRIVER_BASEURL_USER_FRIENDLY = (
        f"http://localhost:{_superset_browser_port_else}/{os.environ.get('SUPERSET_APP_ROOT', '/')}/"
    )
SQLLAB_CTAS_NO_LIMIT = True


def _docker_redshift_loopback_mutator(
    uri: URL,
    params: dict[str, Any],
    username: str | None,
    security_manager: Any,
    source: Any,
) -> tuple[URL, dict[str, Any]]:
    """
    When Superset runs in Docker and Redshift is reached via Teleport (or any TCP
    tunnel) bound to 127.0.0.1 on the *host*, connections must use host.docker.internal
    instead of 127.0.0.1 — inside the container, loopback is not the host.

    Only applies to ``redshift+`` SQLAlchemy URLs. Opt out with
    ``SUPERSET_DOCKER_REDSHIFT_LOOPBACK_REWRITE=false``.
    """
    if os.getenv("SUPERSET_DOCKER_REDSHIFT_LOOPBACK_REWRITE", "true").lower() in (
        "0",
        "false",
        "no",
    ):
        return uri, params
    if os.getenv("DATABASE_HOST") != "db":
        return uri, params
    drivername = uri.drivername or ""
    if not drivername.startswith("redshift"):
        return uri, params
    host = uri.host
    if host not in ("127.0.0.1", "localhost", "::1"):
        return uri, params
    gateway = os.getenv("DOCKER_HOST_GATEWAY_HOST", "host.docker.internal")
    new_uri = uri.set(host=gateway)
    logger.info(
        "DB_CONNECTION_MUTATOR: Redshift host %s -> %s (Docker + Teleport/tunnel on host)",
        host,
        gateway,
    )
    return new_uri, params


log_level_text = os.getenv("SUPERSET_LOG_LEVEL", "INFO")
LOG_LEVEL = getattr(logging, log_level_text.upper(), logging.INFO)

if os.getenv("CYPRESS_CONFIG") == "true":
    # When running the service as a cypress backend, we need to import the config
    # located @ tests/integration_tests/superset_test_config.py
    base_dir = os.path.dirname(__file__)
    module_folder = os.path.abspath(
        os.path.join(base_dir, "../../tests/integration_tests/")
    )
    sys.path.insert(0, module_folder)
    from superset_test_config import *  # noqa

    sys.path.pop(0)

#
# Optionally import superset_config_docker.py (which will have been included on
# the PYTHONPATH) in order to allow for local settings to be overridden
#
try:
    import superset_config_docker
    from superset_config_docker import *  # noqa: F403

    logger.info(
        "Loaded your Docker configuration at [%s]", superset_config_docker.__file__
    )
except ImportError:
    logger.info("Using default Docker config...")

# After optional superset_config_docker import: chain loopback rewrite with any user mutator.
if os.getenv("DATABASE_HOST") == "db":
    _prior_db_connection_mutator = globals().get("DB_CONNECTION_MUTATOR")

    def _chained_db_connection_mutator(
        uri: URL,
        params: dict[str, Any],
        username: str | None,
        security_manager: Any,
        source: Any,
    ) -> tuple[URL, dict[str, Any]]:
        uri2, params2 = _docker_redshift_loopback_mutator(
            uri, params, username, security_manager, source
        )
        if _prior_db_connection_mutator is not None:
            return _prior_db_connection_mutator(
                uri2, params2, username, security_manager, source
            )
        return uri2, params2

    DB_CONNECTION_MUTATOR = _chained_db_connection_mutator
