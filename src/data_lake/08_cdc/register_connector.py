"""Register the Debezium MySQL connector for task 08 (CDC).

Posts the connector configuration to the Kafka Connect REST API so Debezium
starts capturing changes from classicmodels.customers. Debezium first takes a
snapshot of the table (initial load), then streams every subsequent
INSERT/UPDATE/DELETE from the MySQL binlog to the Kafka topic:

    dbserver1.classicmodels.customers

Converter settings keep the messages simple for the Spark pipeline:
    - JsonConverter with schemas disabled  -> the message value is just the
      Debezium envelope {before, after, op, ts_ms, ...} (no schema wrapper).
    - decimal.handling.mode = double        -> creditLimit arrives as a number,
      not a base64-encoded decimal.

Usage:
    uv run python src/data_lake/08_cdc/register_connector.py
"""

import logging
import time

import requests

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("connector")

CONNECT_URL = "http://localhost:8083"
CONNECTOR_NAME = "classicmodels-connector"

CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.debezium.connector.mysql.MySqlConnector",
        "database.hostname": "mysql",
        "database.port": "3306",
        "database.user": "root",
        "database.password": "debezium",
        "database.server.id": "184054",
        "topic.prefix": "dbserver1",
        "database.include.list": "classicmodels",
        "table.include.list": "classicmodels.customers",
        "schema.history.internal.kafka.bootstrap.servers": "kafka:29092",
        "schema.history.internal.kafka.topic": "schemahistory.classicmodels",
        "include.schema.changes": "false",
        "decimal.handling.mode": "double",
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
    },
}


def wait_for_connect(timeout: int = 120) -> None:
    """Block until the Kafka Connect REST API is reachable."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if requests.get(f"{CONNECT_URL}/", timeout=3).status_code == 200:
                log.info("Kafka Connect is up")
                return
        except requests.RequestException:
            pass
        time.sleep(3)
    raise RuntimeError("Kafka Connect did not become ready in time")


def register() -> None:
    """Create (or replace) the Debezium connector."""
    wait_for_connect()

    # Replace any previous instance so re-runs are deterministic.
    requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}", timeout=10)

    resp = requests.post(
        f"{CONNECT_URL}/connectors",
        json=CONFIG,
        headers={"Content-Type": "application/json"},
        timeout=15,
    )
    resp.raise_for_status()
    log.info(f"Connector '{CONNECTOR_NAME}' registered (HTTP {resp.status_code})")

    # Give Debezium a moment to run the initial snapshot.
    time.sleep(10)
    status = requests.get(
        f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status", timeout=10
    ).json()
    state = status.get("connector", {}).get("state")
    log.info(f"Connector state: {state}")


if __name__ == "__main__":
    register()
