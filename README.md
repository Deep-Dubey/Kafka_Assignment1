# Kafka Assignment Project

## Overview

This project demonstrates a simple streaming pipeline using MySQL, Kafka, and Python.

The main flow is:

1. `sql_data_generation.py` populates the MySQL `ecommerce.product` table with sample product data.
2. `producer.py` reads updated rows from MySQL and publishes Avro-encoded records to the Kafka topic `product_updates`.
3. `consumer.py` and `consumer_v2.py` consume messages from `product_updates` and write JSON output files.

## Architecture Diagram

```text
+-------------+       +--------------+       +------------------+
| MySQL       | ----> | producer.py  | ----> | Kafka Topic      |
| ecommerce   |       | (Avro writer)|       | product_updates  |
+-------------+       +--------------+       +------------------+
                                                     |
                                                     |
                             +-----------------------+-----------------------+
                             |                                               |
                             v                                               v
                   +-----------------------+                      +------------------------+
                   | consumer.py           |                      | consumer_v2.py         |
                   | (legacy V1 consumer)  |                      | (V2 consumer with      |
                   |                       |                      |  brand/stock fallback)  |
                   +-----------------------+                      +------------------------+
                             |                                               |
                             v                                               v
                   output/consumer_<partition>.json                 output_v2/consumer_v2_<partition>.json
```

## Components

### `sql_data_generation.py`
- Generates `10000` sample records.
- Inserts data into the MySQL `ecommerce.product` table.
- Fields generated:
  - `id`
  - `name`
  - `category`
  - `price`
  - `last_updated`

### `producer.py`
- Connects to MySQL via `mysql_config.MYSQL_CONFIG`.
- Reads records from `product` where `last_updated > last_timestamp.txt`.
- Encodes each record using `fastavro.schemaless_writer`.
- Publishes to Kafka topic `product_updates` using `confluent-kafka`.
- Keeps `brand` and `stock` fields in the producer schema.
- Updates `last_timestamp.txt` after sending records.

### `consumer.py`
- Consumes from Kafka using `kafka_config.KAFKA_CONFIG`.
- Uses a legacy schema for records without V2 fields.
- Transforms category values to uppercase.
- Applies a 10% discount for `FASHION` category products.
- Writes messages to `output/consumer_<partition>.json`.

### `consumer_v2.py`
- Consumes from the same Kafka topic.
- Supports the V2 `Product` schema with `brand` and `stock` fields.
- Falls back to the legacy schema if the message does not contain V2 fields.
- Sets defaults for missing fields:
  - `brand = "UNKNOWN"`
  - `stock = 0`
- Adds inventory status logic:
  - `LOW_STOCK` when `stock < 20`
  - `AVAILABLE` otherwise
- Writes output to `output_v2/consumer_v2_<partition>.json`.

### `kafka_config.py`
- Contains Kafka connection settings.
- Uses `SASL_SSL` with `PLAIN` authentication for the Kafka cluster.

### `mysql_config.py`
- Contains MySQL connection settings for the `ecommerce` database.

## How to run

1. Activate the virtual environment:
   - PowerShell: `\.venv\Scripts\Activate.ps1`
   - Bash: `source .venv/Scripts/activate`

2. Install Python dependencies:
   - `pip install confluent-kafka mysql-connector-python fastavro`

3. Populate the MySQL table (if needed):
   - `python sql_data_generation.py`

4. Run the producer:
   - `python producer.py`

5. Run consumer V1:
   - `python consumer.py`

6. Run consumer V2:
   - `python consumer_v2.py`

## Output

- `output/` contains legacy consumer JSON files.
- `output_v2/` contains V2 consumer JSON files.

## Notes

- `producer.py` is designed to send incremental updates based on `last_timestamp.txt`.
- The V2 consumer is backward compatible with legacy messages on the topic.
