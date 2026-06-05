import os
import io
import json

from confluent_kafka import Consumer
from fastavro import schemaless_reader

from kafka_config import KAFKA_CONFIG

os.makedirs("output_v2", exist_ok=True)

consumer = Consumer({
    **KAFKA_CONFIG,
    "group.id": "product_group_v2",
    "auto.offset.reset": "earliest"
})

consumer.subscribe(["product_updates"])

schema = {
    "type": "record",
    "name": "Product",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "category", "type": "string"},
        {"name": "price", "type": "float"},
        {"name": "last_updated", "type": "string"},
        {"name": "brand", "type": "string"},
        {"name": "stock", "type": "int"}
    ]
}

legacy_schema = {
    "type": "record",
    "name": "Product",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "name", "type": "string"},
        {"name": "category", "type": "string"},
        {"name": "price", "type": "float"},
        {"name": "last_updated", "type": "string"}
    ]
}

print("Consumer V2 Started...")

while True:

    msg = consumer.poll(1.0)

    if msg is None:
        continue

    if msg.error():
        print(msg.error())
        continue

    buffer = io.BytesIO(msg.value())

    try:
        data = schemaless_reader(
            buffer,
            schema
        )
    except Exception:
        buffer.seek(0)
        data = schemaless_reader(
            buffer,
            legacy_schema
        )
        data["brand"] = "UNKNOWN"
        data["stock"] = 0

    # Existing transformation
    data["category"] = data["category"].upper()

    if data["category"] == "FASHION":
        data["price"] = round(
            data["price"] * 0.9,
            2
        )

    # New V2 Business Logic

    if data["stock"] < 20:
        data["inventory_status"] = "LOW_STOCK"
    else:
        data["inventory_status"] = "AVAILABLE"

    print(data)

    filename = (
        f"output_v2/consumer_v2_"
        f"{msg.partition()}.json"
    )

    with open(
        filename,
        "a",
        encoding="utf-8"
    ) as f:

        f.write(
            json.dumps(data)
        )

        f.write("\n")