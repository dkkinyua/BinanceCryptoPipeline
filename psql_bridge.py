
import os
import psycopg2
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from dotenv import load_dotenv

load_dotenv()

# PostgreSQL connection
conn = psycopg2.connect(
    dbname=os.getenv("DATABASE"),
    user=os.getenv("USER"),
    password=os.getenv("PASSWORD"),
    host=os.getenv("HOST"),
    port=os.getenv("PORT"),
    sslmode='require'
)
cur = conn.cursor()

# Cassandra connection
auth_provider = PlainTextAuthProvider(os.getenv("CASSANDRA_USER"), os.getenv("CASSANDRA_PWD"))
cluster = Cluster([os.getenv("CASSANDRA_ADDRESS")], auth_provider=auth_provider)
session = cluster.connect('binance_test')

# Table definitions
tables = {
    "klines": ["id", "symbol", "open_time", "open_price", "high_price", "low_price", "close_price", "volume", "close_time", "num_trades", "source", "op", "ts_ms", "transaction"],
    "order_book": ["id", "update_id", "symbol", "side", "price", "quantity", "captured_at", "source", "op", "ts_ms", "transaction"],
    "daily_ticker": ["id", "symbol", "open_price", "high_price", "low_price", "last_price", "volume", "quote_volume", "open_time", "close_time", "first_id", "last_id", "count", "source", "op", "ts_ms", "transaction"],
    "recent_trades": ["id", "symbol", "trade_id", "price", "qty", "time", "source", "op", "ts_ms", "transaction"],
    "latest_prices": ["id", "symbol", "price", "time", "source", "op", "ts_ms", "transaction"]
}

def sync_all_data(table, columns):
    print(f"Syncing full data for table: {table}")
    try:
        query = f"SELECT {', '.join(columns)} FROM {table} ALLOW FILTERING"
        rows = session.execute(query)
        insert_query = f"""
            INSERT INTO cassandra.{table} ({', '.join(columns)})
            VALUES ({', '.join(['%s'] * len(columns))})
            ON CONFLICT DO NOTHING
        """
        count = 0
        for row in rows:
            values = tuple(getattr(row, col) for col in columns)
            try:
                cur.execute(insert_query, values)
                count += 1
            except Exception as e:
                print(f"Insert error on {table}: {e}")
        conn.commit()
        print(f"Finished syncing {count} rows for table: {table}")
    except Exception as e:
        print(f"Error querying {table}: {e}")

# Sync each table fully
for table, cols in tables.items():
    sync_all_data(table, cols)

cur.close()
conn.close()
