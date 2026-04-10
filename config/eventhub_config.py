"""
eventhub_config.py
------------------
Azure Event Hubs connection configuration for Spark Structured Streaming.
Uses the Kafka-compatible endpoint of Event Hubs (works with Spark 4.x).
"""

import os


def _extract_namespace(conn_str: str) -> str:
    """Extract the namespace hostname from a connection string."""
    # Endpoint=sb://namespace.servicebus.windows.net/;...
    for part in conn_str.split(";"):
        if part.startswith("Endpoint="):
            return part.replace("Endpoint=sb://", "").rstrip("/")
    return ""


def _extract_sas_key_name(conn_str: str) -> str:
    for part in conn_str.split(";"):
        if part.startswith("SharedAccessKeyName="):
            return part.split("=", 1)[1]
    return ""


def _extract_sas_key(conn_str: str) -> str:
    for part in conn_str.split(";"):
        if part.startswith("SharedAccessKey="):
            return part.split("=", 1)[1]
    return ""


def get_kafka_conf(topic: str) -> dict:
    """
    Return Kafka-compatible configuration for reading from Event Hubs.
    Event Hubs exposes a Kafka endpoint — this works with Spark's built-in
    Kafka connector (no extra JARs needed for Spark 4.x).
    """
    if topic == os.getenv("EVENTHUB_ORDER_TOPIC", "group_6_orders"):
        conn_str = os.environ["EVENTHUB_ORDER_CONNECTION_STRING"]
    else:
        conn_str = os.environ["EVENTHUB_COURIER_CONNECTION_STRING"]

    namespace = _extract_namespace(conn_str)
    sas_key_name = _extract_sas_key_name(conn_str)
    sas_key = _extract_sas_key(conn_str)
    consumer_group = os.getenv("EVENTHUB_CONSUMER_GROUP", "spark-processing")

    # SASL/PLAIN auth string for Event Hubs Kafka endpoint
    sasl_jaas = (
        f'org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" '
        f'password="Endpoint=sb://{namespace};SharedAccessKeyName={sas_key_name};SharedAccessKey={sas_key}";'
    )

    return {
        "kafka.bootstrap.servers": f"{namespace}:9093",
        "subscribe": topic,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": sasl_jaas,
        "kafka.group.id": consumer_group,
        "startingOffsets": "latest",
        "kafka.request.timeout.ms": "60000",
        "kafka.session.timeout.ms": "60000",
    }


# Topic names
ORDER_TOPIC = os.getenv("EVENTHUB_ORDER_TOPIC", "group_6_orders")
COURIER_TOPIC = os.getenv("EVENTHUB_COURIER_TOPIC", "group_6_couriers")
