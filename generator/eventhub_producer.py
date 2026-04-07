"""
eventhub_producer.py
--------------------
Wrapper around azure-eventhub SDK to publish JSON events
to Azure Event Hubs topics.
"""

import json
import os
from azure.eventhub import EventHubProducerClient, EventData


class EventHubProducer:
    """Sends JSON events to an Azure Event Hub topic."""

    def __init__(self, connection_string: str):
        # Connection string already includes EntityPath
        self.producer = EventHubProducerClient.from_connection_string(
            conn_str=connection_string,
        )
        self._batch = None
        self._batch_count = 0
        self._batch_max = 50  # flush every 50 events

    def send(self, event: dict, partition_key: str | None = None):
        """Send a single event. Batches internally for throughput."""
        if self._batch is None:
            self._batch = self.producer.create_batch(
                partition_key=partition_key
            )

        event_data = EventData(json.dumps(event))
        try:
            self._batch.add(event_data)
            self._batch_count += 1
        except ValueError:
            # Batch is full, send it and start a new one
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

    order_producer = EventHubProducer(order_conn)
    courier_producer = EventHubProducer(courier_conn)
    return order_producer, courier_producer
