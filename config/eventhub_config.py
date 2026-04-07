"""
eventhub_config.py
------------------
Azure Event Hubs connection configuration for Spark Structured Streaming.
Uses per-hub connection strings (with EntityPath already included).
"""

import os


def get_eventhub_conf(topic: str) -> dict:
    """
    Return the Spark-compatible Event Hubs configuration dict for a given topic.

    Since we have per-hub connection strings (EntityPath included),
    we pick the right one based on the topic name.
    """
    if topic == os.getenv("EVENTHUB_ORDER_TOPIC", "group_6_orders"):
        conn_str = os.environ["EVENTHUB_ORDER_CONNECTION_STRING"]
    else:
        conn_str = os.environ["EVENTHUB_COURIER_CONNECTION_STRING"]

    return {
        "eventhubs.connectionString": conn_str,
        "eventhubs.consumerGroup": os.getenv("EVENTHUB_CONSUMER_GROUP", "spark-processing"),
        "eventhubs.startingPosition": '{"offset": "-1", "seqNo": -1, "enqueuedTime": null, "isInclusive": true}',
        "maxEventsPerTrigger": os.getenv("EVENTHUB_MAX_EVENTS_PER_TRIGGER", "1000"),
    }


# Topic names
ORDER_TOPIC = os.getenv("EVENTHUB_ORDER_TOPIC", "group_6_orders")
COURIER_TOPIC = os.getenv("EVENTHUB_COURIER_TOPIC", "group_6_couriers")
