import os 
import time
import requests
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
DB_URI = os.getenv("DB_URI")
BASE_URL = 'https://api.binance.com'
symbols = [
    "BTCUSDT",   
    "ETHUSDT",
    "BNBUSDT",
    "XRPUSDT",
    "SOLUSDT",
    "ADAUSDT",
    "DOGEUSDT",
    "MATICUSDT",
    "DOTUSDT",
    "AVAXUSDT"
]

# get order book
def get_depth(BASE_URL, symbols, DB_URI):
    coin_data = []
    url = f"{BASE_URL}/api/v3/depth"
    for s in symbols:
        params = {'symbol': s, 'limit': 10}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            for side, orders in [('bid', data['bids']), ('ask', data['asks'])]:
                for price, qty in orders:
                    coin_data.append({
                        'update_id': data['lastUpdateId'],
                        'symbol': s,
                        'side': side,
                        'price': float(price),
                        'quantity': float(qty),
                        'captured_at': int(time.time() * 1000)
                    })
        else:
            print(f"Error fetching order book: {response.status_code}, {response.text}")
    
    df = pd.DataFrame(coin_data)
    df["captured_at"] = pd.to_datetime(df["captured_at"], unit='ms')
    try:
        engine = create_engine(DB_URI)
        df.to_sql('order_book', con=engine, schema='binance', index=False, if_exists='append')
        print("Order book data loaded successfully!")
    except Exception as e:
        print(f"Error loading order book into db: {e}")

# get recent trades from API
def get_recent_trades(BASE_URL, symbols, DB_URI):
    coin_data = []
    url = f"{BASE_URL}/api/v3/trades"
    for s in symbols:
        params = {'symbol': s, 'limit': 10}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            for d in data:
                coin_data.append({
                    'symbol': s,
                    'trade_id': d["id"],
                    'price': d["price"],
                    'qty': d['qty'],
                    'time': d['time']
                })
        else:
            print(f"Error fetching recent trades: {response.status_code}, {response.text}")
    df = pd.DataFrame(coin_data)
    df["price"] = df["price"].astype(float)
    df["qty"] = df["qty"].astype(float)
    df['time'] = pd.to_datetime(df['time'], unit='ms')
    try:
        engine = create_engine(DB_URI)
        df.to_sql('recent_trades', con=engine, schema='binance', index=False, if_exists='append')
        print("Recent trades data loaded successfully!")
    except Exception as e:
        print(f"Loading error into databases: {e}")

# get candlestick data
def get_klines(BASE_URL, symbols, DB_URI):
    coin_data = []
    url = f"{BASE_URL}/api/v3/klines"
    for s in symbols:
        params = {'symbol': s, 'limit': 10, 'interval': '30m'}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            for d in data:
                coin_data.append({
                        'symbol': s,
                        'open_time': d[0],
                        'open_price': float(d[1]),
                        'high_price': float(d[2]),
                        'low_price': float(d[3]),
                        'close_price': float(d[4]),
                        'volume': float(d[5]),
                        'close_time': d[6],
                        'num_trades': d[8]
                    })
        else:
            print(f"Error fetching recent trades: {response.status_code}, {response.text}")

    df = pd.DataFrame(coin_data)
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
    try:
        engine = create_engine(DB_URI)
        df.to_sql("klines", con=engine, schema='binance', index=False, if_exists='append')
        print("Klines data loaded successfully!")
    except Exception as e:
        print(f"Loading kline data into database: {e}")

# get 24h ticker price
def get_daily_ticker(BASE_URL, symbols):
    coin_data = []
    url = f"{BASE_URL}/api/v3/ticker/24hr"
    for s in symbols:
        params = {'symbol': s, 'type': 'MINI'}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            coin_data.append(data)
        else:
            print(f"Error fetching recent trades: {response.status_code}, {response.text}")
    
    df = pd.DataFrame(coin_data)
    
    df['openPrice'] = df['openPrice'].astype(float)
    df['highPrice'] = df['highPrice'].astype(float)
    df['lowPrice'] = df['lowPrice'].astype(float)
    df['lastPrice'] = df['lastPrice'].astype(float)
    df['volume'] = df['volume'].astype(float)
    df['openTime'] = pd.to_datetime(df['openTime'], unit='ms')
    df['closeTime'] = pd.to_datetime(df['closeTime'], unit='ms')

    df = df.rename(columns={
            'openPrice': 'open_price',
            'highPrice': 'high_price',
            'lowPrice': 'low_price',
            'lastPrice': 'last_price',
            'quoteVolume': 'quote_volume',
            'openTime': 'open_time',
            'closeTime': 'close_time',
            'firstId': 'first_id',
            'lastId': 'last_id'
        })

    try:
        engine = create_engine(DB_URI)
        df.to_sql('daily_ticker', con=engine, schema='binance', index=False, if_exists='append')
        print("Daily Ticker data loaded successfully")
    except Exception as e:
        print(f"Loading daily ticker data into db: {e}")

# get latest prices
def get_latest_prices(BASE_URL, symbols, DB_URI):
    coin_data = []
    url = f"{BASE_URL}/api/v3/ticker/price"
    for s in symbols:
        params = {'symbol': s}
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            coin_data.append({
                'symbol': s,
                'price': data["price"],
                'time': int(time.time() * 1000)
            })
        else:
            print(f"Error fetching recent trades: {response.status_code}, {response.text}")
    df = pd.DataFrame(coin_data)
    df["price"] = df["price"].astype(float)
    df["time"] = pd.to_datetime(df["time"], unit='ms')
    try:
        engine = create_engine(DB_URI)
        df.to_sql('latest_prices', con=engine, schema='binance', index=False, if_exists='append')
        print("Latest price data loaded successfully!")
    except Exception as e:
        print(f"Loading latest prices to db: {e}")


def main():
    print("Binance data extraction and loading starting now...")

    get_latest_prices(BASE_URL, symbols, DB_URI)
    get_daily_ticker(BASE_URL, symbols)
    get_klines(BASE_URL, symbols, DB_URI)
    get_recent_trades(BASE_URL, symbols, DB_URI)
    get_depth(BASE_URL, symbols, DB_URI)

    print("Process complete!")

main()