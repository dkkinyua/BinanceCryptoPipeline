# Binance Crypto Data Pipeline
This project implements a pipeline to extract cryptocurrency market data from Binance, load it into a PostgreSQL database, capture database changes by Change Data Capture (CDC) method using Debezium's `postgrescdcconnector` CDC connector and Kafka, and stream the data into Apache Cassandra using PySpark Streaming.

## Project Requirements 
This project requires the following:
- `python 3.x`
- `pyspark 3.5.x`
- `cassandra-driver`
- `confluent-kafka`
- `sqlalchemy`
- `psycopg2-binary`
- Confluent Cloud cluster
- Debezium's PostgresCDCConnectorV2 connector, available on Confluent Cloud
- Microsoft Azure Virtual Machine to host our Apache Cassandra. (You can host it locally if you want to)

## Project Workflow

The flowchart below illustrates the porject workflow and steps taken to ensure data delivery success

![Project Workflow](https://res.cloudinary.com/depbmpoam/image/upload/v1751298259/Screenshot_2025-06-30_184240_fgvvs0.png)

## Project Setup

To clone this project, head over to your terminal and run:
```bash
git clone https://github.com/dkkinyua/BinanceCryptoPipeline
```

Run the following commands to change into the cloned repository, install a virtual environment and install required dependencies for this project:

```bash
cd BinanceCryptoPipeline

python -m venv myenv
source myenv/bin/activate # LInux/MacOS
myenv\Scripts\activate # Windows/Powershell

pip install -r requirements.txt # Install dependencies
```

## Project Overview
### 1. Extraction, Transformation and Loading of data into a PostgreSQL database.
**Script:** `extraction_script.py`

This script extracts and transforms 5 types of data from Binance REST API:

- Recent Trades

- Order Book Depth

- 30-minute Klines / Candlesticks

- 24h Daily Ticker prices

- Latest Prices

After transformation, it loads the data into a PostgreSQL database's `Binance` schema. The PostgreSQL tables created are:

- `binance.recent_trades`

- `binance.order_book`

- `binance.klines`

- `binance.daily_ticker`

- `binance.latest_prices`

### 2. Change Data Capture with Debezium + Kafka

Debezium PostgreSQL Connector monitors all tables in the binance schema. It monitors any changes in the tables e.g. `INSERT`, `UPDATE` or `DELETE` changes in the database and stores these changes inside topics as logs.

Postgres database during changes creates Write Ahead Logs (WALs) which show the changes that have occur for easier monitoring of data changes in the database. These Write Ahead Logs are stored in a slot allocated inside a database.

The PostgresCDCSourceConnector used in this project has been hosted and connected on Confluent Kafka. Head over to Confluent Cloud and sign in. Create a new environment and define a new cluster. Head over to your new cluster and search for the `PostgresCDCSourceConnectorV2` connector and folow the instructions to connect Kafka to your PostgreSQL database.

### 3. PySpark Streaming Job
**Script:** `spark_streaming.py`

This script contains a Spark Streaming job that acts as a data sink into Apache Cassandra.
It consumes the logs from the various Kafka topics into JSON format, flattens after, source, op, ts_ms, and transaction fields, transforms them and loads them into Cassandra hosted on an Azure Ubuntu VM.

## Conclusion

This project demonstrates a complete end-to-end real-time data pipeline using Python and Apache Spark. It extracts data from a PostgreSQL database, captures change events via Debezium CDC through Kafka, and streams the transformed data into an Apache Cassandra database for scalable storage and analytics. The pipeline ensures near real-time synchronization between PostgreSQL and Cassandra, making it suitable for real-time reporting, monitoring, and big data analytics use cases.



