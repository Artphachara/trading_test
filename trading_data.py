###########
# Imports #
###############################################################################

import logging
import os
import sqlite3

import yliveticker

#############
# Constants #
###############################################################################

logging.basicConfig(level=logging.INFO)

conn = sqlite3.connect(os.path.join("trading_data.db"))
cursor = conn.cursor()

cursor.execute('''
CREATE TABLE IF NOT EXISTS TickPrices (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ticker TEXT NOT NULL,
    exchange TEXT NOT NULL,
    price REAL NOT NULL,
    timestamp INTEGER NOT NULL,
    marketHours INTEGER,
    changePercent REAL,
    volume INTEGER,
    dayVolume INTEGER,
    change REAL,
    priceHint INTEGER
);
''')

conn.commit()

#############
# Functions #
###############################################################################

def save_to_db(msg: dict) -> None:
    """
    Inserts real-time ticker data into the TickPrices table.

    This function takes a dictionary containing real-time stock
    trading data and inserts it into the SQLite database table `TickPrices`.
    It logs an error if any issue occurs during the database operation.

    Parameters
    ----------
    msg : dict
        A dictionary containing the real-time data for a ticker. 
        The dictionary is expected to have the following keys:
        - id : str
            Ticker symbol (e.g., "CPALL.BK", "BTC-USD").
        - exchange : str
            The exchange where the ticker is listed (e.g., "SET").
        - price : float
            The current price of the ticker.
        - timestamp : int
            UNIX timestamp of the data point.
        - marketHours : int
            Market hours indicator (e.g., premarket, regular, after-hours).
        - changePercent : float
            Percentage change in the price.
        - volume : int
            Current trading volume.
        - dayVolume : int
            Total trading volume for the day.
        - change : float
            Absolute change in the price.
        - priceHint : int
            Granularity of the price.

    Raises
    ------
    Exception
        Logs an error if there is an issue during the database insertion.
    """

    try:
        cursor.execute('''
        INSERT INTO TickPrices (ticker, exchange, price, timestamp, \
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
    
###############################################################################

def on_new_msg(ws, msg):
    """
    Callback function to handle incoming ticker updates.

    This function is triggered whenever a new message is received from the 
    WebSocket. It logs the message and saves the data to the database.

    Parameters
    ----------
    ws : websocket.WebSocketApp
        The WebSocket connection object used for receiving messages.

    msg : dict
        A dictionary containing the real-time data for a ticker. 
        The dictionary is expected to have the following keys:
        - id : str
            Ticker symbol (e.g., "CPALL.BK", "BTC-USD").
        - exchange : str
            The exchange where the ticker is listed (e.g., "SET").
        - price : float
            The current price of the ticker.
        - timestamp : int
            UNIX timestamp of the data point.
        - marketHours : int
            Market hours indicator (e.g., premarket, regular, after-hours).
        - changePercent : float
            Percentage change in the price.
        - volume : int
            Current trading volume.
        - dayVolume : int
            Total trading volume for the day.
        - change : float
            Absolute change in the price.
        - priceHint : int
            Granularity of the price.

    Raises
    ------
    Exception
        Logs an error if there is an issue during message processing or database insertion.
    """
    
    try:
        logging.info(msg)
        save_to_db(msg)
    except Exception as e:
        logging.error(f"Error processing message: {e}")

########
# Main #
###############################################################################

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
