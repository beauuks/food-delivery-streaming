"""
eventhub_producer.py
--------------------
Wrapper around azure-eventhub SDK to publish AVRO-serialized events
to Azure Event Hubs topics.
"""

import os
from azure.eventhub import EventHubProducerClient, EventData
from avro_utils import serialize_to_avro_bytes


# Schema file mapping per feed type
_SCHEMA_FILES = {
    "order": "order_lifecycle_event.avsc",
    "courier": "courier_status_event.avsc",
}


class EventHubProducer:
    """Sends AVRO-serialized events to an Azure Event Hub topic."""

    def __init__(self, connection_string: str, feed_type: str):
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_string,
        )
        self.feed_type = feed_type
        self.schema_file = _SCHEMA_FILES[feed_type]
        self._batch = None
        self._batch_count = 0
        self._batch_max = 50

    def send(self, event: dict, partition_key: str | None = None):
        """Send a single event as AVRO bytes. Batches internally for throughput."""
        if self._batch is None:
            self._batch = self.producer.create_batch(
                partition_key=partition_key
            )

        avro_bytes = serialize_to_avro_bytes(event, self.schema_file)
        event_data = EventData(avro_bytes)
        try:
            self._batch.add(event_data)
            self._batch_count += 1
        except ValueError:
            self.flush()
            self._batch = self.producer.create_batch(
                partition_key=partition_key
            )
            self._batch.add(event_data)
            self._batch_count = 1

        if self._batch_count >= self._batch_max:
            self.flush()

    def flush(self):
        """Send any pending batch."""
        if self._batch and self._batch_count > 0:
            self.producer.send_batch(self._batch)
            self._batch = None
            self._batch_count = 0

    def close(self):
        """Flush remaining events and close the producer."""
        self.flush()
        self.producer.close()


def create_producers() -> tuple["EventHubProducer", "EventHubProducer"]:
    """Create order and courier Event Hub producers from environment variables."""
    order_conn = os.environ["EVENTHUB_ORDER_CONNECTION_STRING"]
    courier_conn = os.environ["EVENTHUB_COURIER_CONNECTION_STRING"]

    order_producer = EventHubProducer(order_conn, feed_type="order")
    courier_producer = EventHubProducer(courier_conn, feed_type="courier")
    return order_producer, courier_producer
