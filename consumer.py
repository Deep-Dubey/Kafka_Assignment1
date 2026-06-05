import io
import json
import os

from confluent_kafka import Consumer
from fastavro import schemaless_reader

from kafka_config import KAFKA_CONFIG

os.makedirs("output", exist_ok=True)

consumer = Consumer({

    **KAFKA_CONFIG,

    "group.id":
    "product_group",

    "auto.offset.reset":
    "earliest"
})

consumer.subscribe(
    ["product_updates"]
)

schema = {
    "type":"record",
    "name":"Product",
    "fields":[
        {"name":"id","type":"int"},
        {"name":"name","type":"string"},
        {"name":"category","type":"string"},
        {"name":"price","type":"float"},
        {"name":"last_updated","type":"string"}
    ]
}

while True:

    msg = consumer.poll(1)

    if msg is None:
        continue

    if msg.error():
        continue

    buffer = io.BytesIO(
        msg.value()
    )

    data = schemaless_reader(
        buffer,
        schema
    )

    data["category"] = (
        data["category"]
        .upper()
    )

    if (
        data["category"]
        == "FASHION"
    ):
        data["price"] *= 0.9

    filename = (
        f"output/"
        f"consumer_"
        f"{msg.partition()}"
        f".json"
    )

    with open(
        filename,
        "a"
    ) as f:

        f.write(
            json.dumps(data)
        )

        f.write("\n")