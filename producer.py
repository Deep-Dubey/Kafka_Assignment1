import mysql.connector
import json
import io

from confluent_kafka import Producer
from fastavro import schemaless_writer

from mysql_config import MYSQL_CONFIG
from kafka_config import KAFKA_CONFIG

producer = Producer(KAFKA_CONFIG)

conn = mysql.connector.connect(
    **MYSQL_CONFIG
)

cursor = conn.cursor(dictionary=True)

with open(
    "last_timestamp.txt",
    "r"
) as f:
    last_ts = f.read().strip()

query = """
SELECT *
FROM product
WHERE last_updated > %s
ORDER BY last_updated
"""

cursor.execute(
    query,
    (last_ts,)
)

rows = cursor.fetchall()

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

latest_ts = last_ts

for row in rows:

    buffer = io.BytesIO()

    record = {
    "id": row["id"],
    "name": row["name"],
    "category": row["category"],
    "price": row["price"],
    "last_updated": str(row["last_updated"]),
    "brand": row["brand"],
    "stock": row["stock"]
}

    schemaless_writer(
        buffer,
        schema,
        record
    )

    producer.produce(
        topic="product_updates",
        key=str(row["id"]),
        value=buffer.getvalue()
    )

    latest_ts = str(
        row["last_updated"]
    )

producer.flush()

with open(
    "last_timestamp.txt",
    "w"
) as f:
    f.write(latest_ts)

print(
    f"Sent {len(rows)} records"
)