import logging
import os
import sqlite3
import time
import threading
from flask import Flask, request, jsonify

import yliveticker

#############
# Constants #
###############################################################################

logging.basicConfig(level=logging.INFO)

# SQLite database connection
conn = sqlite3.connect(os.path.join("trading_data.db"), check_same_thread=False)
cursor = conn.cursor()

# Create OneMinBars table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS OneMinBars (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    exchange TEXT NOT NULL,
    minPrice REAL NOT NULL,
    maxPrice REAL NOT NULL,
    firstPrice REAL NOT NULL,
    lastPrice REAL NOT NULL,
    timestamp INTEGER NOT NULL,
    volume INTEGER,
    dayVolume INTEGER
);
''')
conn.commit()

# Initialize Flask app
app = Flask(__name__)

#############
# Functions #
###############################################################################

def save_to_db(msg: dict) -> None:
    """
    Inserts real-time ticker data into the TickPrices table.

    Parameters
    ----------
    msg : dict
        A dictionary containing the real-time data for a ticker.
    """
    try:
        cursor.execute('''
        INSERT INTO TickPrices (ticker, exchange, price, timestamp, 
        marketHours, changePercent, volume, dayVolume, change, priceHint)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            msg.get("id"),
            msg.get("exchange"),
            msg.get("price"),
            msg.get("timestamp"),
            msg.get("marketHours"),
            msg.get("changePercent"),
            msg.get("volume"),
            msg.get("dayVolume"),
            msg.get("change"),
            msg.get("priceHint"),
        ))
        conn.commit()
    except Exception as e:
        logging.error(f"Database error: {e}")

def save_minute_bars():
    """
    Calculates minute-wise aggregates (min, max, first, and last prices)
    for each ticker from the TickPrices table and inserts the results into the OneMinBars table.
    """
    current_time = int(time.time())

    # Get distinct tickers from the TickPrices table
    cursor.execute('SELECT DISTINCT ticker FROM TickPrices')
    tickers = cursor.fetchall()

    for ticker in tickers:
        ticker = ticker[0]  # Get the ticker symbol

        # Query the TickPrices table for the price information grouped by minute
        cursor.execute('''
            SELECT
                strftime('%Y-%m-%d %H:%M', timestamp) AS minute,
                MIN(price) AS minPrice,
                MAX(price) AS maxPrice,
                FIRST(price) AS firstPrice,
                LAST(price) AS lastPrice,
                SUM(volume) AS volume,
                SUM(dayVolume) AS dayVolume
            FROM TickPrices
            WHERE ticker = ?
            GROUP BY strftime('%Y-%m-%d %H:%M', timestamp)
        ''', (ticker,))
        
        results = cursor.fetchall()

        for result in results:
            minute, min_price, max_price, first_price, last_price, volume, day_volume = result
            timestamp = int(time.mktime(time.strptime(minute, '%Y-%m-%d %H:%M')))  # Convert the minute to timestamp

            cursor.execute('''
                INSERT INTO OneMinBars (ticker, exchange, minPrice, maxPrice, firstPrice, lastPrice, timestamp, volume, dayVolume)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                ticker,
                "SET",  # You can customize this field as needed
                min_price,
                max_price,
                first_price,
                last_price,
                timestamp,
                volume,
                day_volume
            ))

            conn.commit()

    logging.info("Minute bars saved successfully.")

@app.route('/api/one_min_bars', methods=['GET'])
def get_minute_bars():
    """
    REST API to get minute-wise trading statistics for a specified ticker and time range.
    Expects 'ticker', 'start_time', and 'end_time' as query parameters.
    """
    ticker = request.args.get('ticker')
    start_time = request.args.get('start_time', type=int)
    end_time = request.args.get('end_time', type=int)

    if not ticker or not start_time or not end_time:
        return jsonify({"error": "Missing required parameters. Please provide 'ticker', 'start_time', and 'end_time'."}), 400

    # Query the OneMinBars table to retrieve the data
    cursor.execute('''
        SELECT ticker, minPrice, maxPrice, firstPrice, lastPrice, timestamp, volume, dayVolume
        FROM OneMinBars
        WHERE ticker = ? AND timestamp BETWEEN ? AND ?
        ORDER BY timestamp
    ''', (ticker, start_time, end_time))

    rows = cursor.fetchall()

    if not rows:
        return jsonify({"error": "No data found for the given parameters."}), 404

    # Prepare the response data
    result = []
    for row in rows:
        result.append({
            "ticker": row[0],
            "minPrice": row[1],
            "maxPrice": row[2],
            "firstPrice": row[3],
            "lastPrice": row[4],
            "timestamp": row[5],
            "volume": row[6],
            "dayVolume": row[7]
        })

    return jsonify(result)

def on_new_msg(ws, msg):
    """
    Callback function to handle incoming ticker updates.

    Parameters
    ----------
    ws : websocket.WebSocketApp
        The WebSocket connection object used for receiving messages.

    msg : dict
        A dictionary containing the real-time data for a ticker.
    """
    try:
        logging.info(msg)
        save_to_db(msg)
    except Exception as e:
        logging.error(f"Error processing message: {e}")

def periodic_save():
    """
    Periodically saves the minute-wise bars every minute.
    """
    while True:
        save_minute_bars()
        time.sleep(60)  # Wait for 1 minute

#############
# Main #
###############################################################################

# Run the periodic_save function in a separate thread to save minute-wise bars
thread = threading.Thread(target=periodic_save)
thread.daemon = True
thread.start()

# WebSocket to receive real-time ticker data
yliveticker.YLiveTicker(
    on_ticker=on_new_msg,
    ticker_names=[
        "CPALL.BK",
        "PTT.BK",
        "THB=X",
        "BTC-USD",
        "^GSPC",
        "ES=F",
        "CL=F",
        "GC=F"
    ]
)

# Start Flask app to handle REST API requests
if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
