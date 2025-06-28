import os
import time
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, LongType, FloatType, StringType, TimestampType, IntegerType, DoubleType

load_dotenv()

spark = SparkSession.builder \
    .appName("BinancetoCassandraDataPipeline") \
    .config("spark.cassandra.connection.host", "20.38.45.22") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
            "com.datastax.spark:spark-cassandra-connector_2.13:3.5.0") \
    .getOrCreate()

latest_prices_schema = StructType([
    StructField("before", StructType([
        StructField("id", LongType()), StructField("symbol", StringType()),
        StructField("price", StringType()), StructField("time", LongType())
    ])),
    StructField("after", StructType([
        StructField("id", LongType()), StructField("symbol", StringType()),
        StructField("price", StringType()), StructField("time", LongType())
    ])),
    StructField("source", StructType(), True),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("transaction", StructType(), True)
])

klines_schema = StructType([
    StructField("before", StructType([
        StructField("id", LongType()), StructField("symbol", StringType()),
        StructField("open_time", LongType()), StructField("open_price", DoubleType()),
        StructField("high_price", DoubleType()), StructField("low_price", DoubleType()),
        StructField("close_price", DoubleType()), StructField("volume", DoubleType()),
        StructField("close_time", LongType()), StructField("num_trades", IntegerType())
    ])),
    StructField("after", StructType([
        StructField("id", LongType()), StructField("symbol", StringType()),
        StructField("open_time", LongType()), StructField("open_price", DoubleType()),
        StructField("high_price", DoubleType()), StructField("low_price", DoubleType()),
        StructField("close_price", DoubleType()), StructField("volume", DoubleType()),
        StructField("close_time", LongType()), StructField("num_trades", IntegerType())
    ])),
    StructField("source", StructType(), True),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("transaction", StructType(), True)
])

ticker_schema = StructType([
    StructField("before", StructType([
        StructField("id", LongType()), StructField("symbol", StringType()),
        StructField("open_price", DoubleType()), StructField("high_price", DoubleType()),
        StructField("low_price", DoubleType()), StructField("last_price", StringType()),
        StructField("volume", DoubleType()), StructField("quote_volume", StringType()),
        StructField("open_time", LongType()), StructField("close_time", LongType()),
        StructField("first_id", LongType()), StructField("last_id", LongType()),
        StructField("count", LongType())
    ])),
    StructField("after", StructType([
        StructField("id", LongType()), StructField("symbol", StringType()),
        StructField("open_price", DoubleType()), StructField("high_price", DoubleType()),
        StructField("low_price", DoubleType()), StructField("last_price", StringType()),
        StructField("volume", DoubleType()), StructField("quote_volume", StringType()),
        StructField("open_time", LongType()), StructField("close_time", LongType()),
        StructField("first_id", LongType()), StructField("last_id", LongType()),
        StructField("count", LongType())
    ])),
    StructField("source", StructType(), True),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("transaction", StructType(), True)
])

book_schema = StructType([
    StructField("before", StructType([
        StructField("id", LongType()), StructField("update_id", LongType()),
        StructField("symbol", StringType()), StructField("side", StringType()),
        StructField("price", DoubleType()), StructField("quantity", DoubleType()),
        StructField("captured_at", LongType())
    ])),
    StructField("after", StructType([
        StructField("id", LongType()), StructField("update_id", LongType()),
        StructField("symbol", StringType()), StructField("side", StringType()),
        StructField("price", DoubleType()), StructField("quantity", DoubleType()),
        StructField("captured_at", LongType())
    ])),
    StructField("source", StructType(), True),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("transaction", StructType(), True)
])

rts_schema = StructType([
    StructField("before", StructType([
        StructField("id", LongType()), StructField("symbol", StringType()),
        StructField("trade_id", LongType()), StructField("price", DoubleType()),
        StructField("qty", DoubleType()), StructField("time", LongType())
    ])),
    StructField("after", StructType([
        StructField("id", LongType()), StructField("symbol", StringType()),
        StructField("trade_id", LongType()), StructField("price", DoubleType()),
        StructField("qty", DoubleType()), StructField("time", LongType())
    ])),
    StructField("source", StructType(), True),
    StructField("op", StringType()),
    StructField("ts_ms", LongType()),
    StructField("transaction", StructType(), True)
])

topics_list = [
    "binance.binance.recent_trades",
    "binance.binance.order_book",
    "binance.binance.daily_ticker",
    "binance.binance.klines",
    "binance.binance.latest_prices"
]

schema_list = [rts_schema, book_schema, ticker_schema, klines_schema, latest_prices_schema]
tables = ["recent_trades", "order_book", "daily_ticker", "klines", "latest_prices"]

def process_schema(table, topic, schema):
    df = spark.readStream \
        .format('kafka') \
        .option('kafka.bootstrap.servers', os.getenv("CONFLUENT_SERVER")) \
        .option('subscribe', topic) \
        .option('startingOffsets', 'earliest') \
        .option('kafka.security.protocol', 'SASL_SSL') \
        .option('kafka.sasl.mechanism', 'PLAIN') \
        .option('kafka.sasl.jaas.config', f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{os.getenv("CONFLUENT_API_KEY")}" password="{os.getenv("CONFLUENT_SECRET_KEY")}";') \
        .load()

    parse_df = df.selectExpr("CAST(value as string) AS value") \
                 .select(from_json(col("value"), schema).alias("data")) \
                 .filter(col("data.op") == "c") \
                 .filter(col("data.after").isNotNull())

    flattened_cols = [f"data.after.{field.name}" for field in schema["after"].dataType.fields]
    meta_cols = ["data.source", "data.op", "data.ts_ms", "data.transaction"]

    final_df = parse_df.select(
        *[col(c).alias(c.split(".")[-1]) for c in flattened_cols],
        col("data.source").cast("string").alias("source"),
        col("data.op").alias("op"),
        col("data.ts_ms").alias("ts_ms"),
        col("data.transaction").cast("string").alias("transaction")
    )

    def write_to_cassandra(df, batch_id):
        df.write \
          .format("org.apache.spark.sql.cassandra") \
          .mode("append") \
          .options(table=table, keyspace="binance_test") \
          .save()

    query = final_df.writeStream \
        .foreachBatch(write_to_cassandra) \
        .outputMode("update") \
        .start()

    print(f"Started streaming for {topic} -> {table}")
    return query

def stream_topics(process_func, topics, schemas, tables):
    queries = []
    for topic, schema, table in zip(topics, schemas, tables):
        query = process_func(table, topic, schema)
        queries.append(query)
        time.sleep(15)
    return queries

queries = stream_topics(process_schema, topics_list, schema_list, tables)

for query in queries:
    query.awaitTermination()
