"""
avro_utils.py
-------------
Utilities for serializing events to AVRO format using fastavro.
Falls back to JSON if fastavro is not installed (for environments without it).
"""

import json
import io
import os
from typing import Dict, Any, List

try:
    import fastavro
    from fastavro import writer, parse_schema
    AVRO_AVAILABLE = True
except ImportError:
    AVRO_AVAILABLE = False
    print("[avro_utils] fastavro not installed. AVRO output will be skipped.")


_SCHEMA_DIR = os.path.join(os.path.dirname(__file__), "..", "schemas")

_parsed_schemas: Dict[str, Any] = {}


def _load_schema(schema_file: str) -> Any:
    if schema_file not in _parsed_schemas:
        path = os.path.join(_SCHEMA_DIR, schema_file)
        with open(path) as f:
            raw = json.load(f)
        _parsed_schemas[schema_file] = parse_schema(raw) if AVRO_AVAILABLE else raw
    return _parsed_schemas[schema_file]


def _prepare_record(event: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert Python event dict to AVRO-compatible record.
    Handles None -> null unions and enum validation.
    """
    record = dict(event)

    # AVRO union fields: None must stay as None (fastavro handles null unions automatically)
    # Convert long ints for timestamp fields (already millis)
    for ts_field in ("event_time", "ingestion_time", "session_start_time"):
        if ts_field in record and record[ts_field] is not None:
            record[ts_field] = int(record[ts_field])

    return record


def serialize_order_events_to_avro(events: List[Dict[str, Any]], output_path: str) -> bool:
    """Write a list of order lifecycle events to an AVRO file."""
    if not AVRO_AVAILABLE:
        return False
    schema = _load_schema("order_lifecycle_event.avsc")
    records = [_prepare_record(e) for e in events]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wb") as f:
        writer(f, schema, records)
    return True


def serialize_courier_events_to_avro(events: List[Dict[str, Any]], output_path: str) -> bool:
    """Write a list of courier status events to an AVRO file."""
    if not AVRO_AVAILABLE:
        return False
    schema = _load_schema("courier_status_event.avsc")
    records = [_prepare_record(e) for e in events]
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wb") as f:
        writer(f, schema, records)
    return True


def serialize_to_avro_bytes(event: Dict[str, Any], schema_file: str) -> bytes:
    """Serialize a single event to AVRO bytes (for streaming to Kafka/Event Hubs)."""
    if not AVRO_AVAILABLE:
        return json.dumps(event).encode()
    schema = _load_schema(schema_file)
    buf = io.BytesIO()
    writer(buf, schema, [_prepare_record(event)])
    return buf.getvalue()
