#!/usr/bin/env python3
import datetime
import logging
import random
import uuid

import time_uuid
from cassandra.query import BatchStatement

# Set logger
log = logging.getLogger()

# Create keySpace and tables
#-------------------------------->
""" 
* This module contains the queries to create the keyspace and tables in Cassandra.
* The params that are passed to the queries are the keyspace name and the replication factor.
* The queries can be identified by the {} placeholders.
"""
CREATE_KEYSPACE = """ 
CREATE KEYSPACE IF NOT EXISTS {} 
WITH REPLICATION = {{ 'class' : 'SimpleStrategy', 'replication_factor' : {} }};
"""

CREATE_USERS_TABLE = """
    CREATE TABLE IF NOT EXISTS accounts_by_user (
        username TEXT,
        account_number TEXT,
        cash_balance DECIMAL,
        name TEXT STATIC,
        PRIMARY KEY ((username),account_number)
    )
"""

CREATE_POSSITIONS_BY_ACCOUNT_TABLE = """
    CREATE TABLE IF NOT EXISTS positions_by_account (
        account TEXT,
        symbol TEXT,
        quantity DECIMAL,
        PRIMARY KEY ((account),symbol)
    )
"""

CREATE_TRADES_BY_ACCOUNT_DATE_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_d (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), trade_id)
    ) WITH CLUSTERING ORDER BY (trade_id DESC)
"""

#   NEW TABLES HERE --------------------------->
CREATE_TRADES_BY_ACCOUNT_TD_TABLE = """ 
    CREATE TABLE IF NOT EXISTS trades_by_a_td (
            account TEXT,
            trade_id TIMEUUID,
            type TEXT,
            symbol TEXT,
            shares DECIMAL,
            price DECIMAL,
            amount DECIMAL,
            PRIMARY KEY ((account), type, trade_id)
        ) WITH CLUSTERING ORDER BY (trade_id DESC)
    """
CREATE_TABLE_BY_ACCOUNT_STD_TABLE = """
    CREATE TABLE IF NOT EXISTS trades_by_a_std (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), trade_id, type, symbol)
    ) WITH CLUSTERING ORDER BY (trade_id DESC)
"""

CREATE_TABLE_BY_ACCOUNT_SD_TABLE = """ 
    CREATE TABLE IF NOT EXISTS trades_by_a_sd (
        account TEXT,
        trade_id TIMEUUID,
        type TEXT,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), trade_id, symbol)
    ) WITH CLUSTERING ORDER BY (trade_id DESC)
"""

# <--------------------------------

# Insert queries
#-------------------------------->
# select all accounts for a user
# Q1
SELECT_USER_ACCOUNTS = """
    SELECT username, account_number, name, cash_balance
    FROM accounts_by_user
    WHERE username = ?
"""
# find all positions in an account, ordered by symbol
# Q2
SELECT_POSITIONS_BY_ACCOUNT = """ 
    SELECT symbol, quantity FROM positions_by_account 
    WHERE account = ?
    ORDER BY symbol ASC;
"""
# Find all trades for an account, with optional date range, 
# transaction type, and stock symbol, ordered by trade date (DESC)
# Q3
SELECT_TRADES_BY_ACOUNT_QUERY = """ 
    SELECT * FROM trades_by_a_d
    WHERE account = ?
    AND trade_id >= minTimeuuid(?)
    AND trade_id <= maxTimeuuid(?)
    AND type = ?
    AND symbol = ?
    ORDER BY trade_id DESC;
"""

# find all trades in an account, transaction type and symbol, ordered by trade_id
# Q3.1
SELECT_TRADES_BY_ACCOUNT_DATE = """
    SELECT * FROM trades_by_a_d WHERE account = ? 
    ORDER BY trade_id DESC;
"""
# Q3.2
SELECT_TRADES_BY_ACCOUNT_DATE_RANGE = """
    SELECT * FROM trades_by_a_d
    WHERE account = ?
    AND trade_id >= minTimeuuid(?)
    AND trade_id <= maxTimeuuid(?)
    ORDER BY trade_id DESC;
"""
# Q3.3
SELECT_TRADES_BY_ACCOUNT_DATE_RANGE_TYPE = """
    SELECT * FROM trades_by_a_td
    WHERE account = ?
    AND type = ?
    AND trade_id >= minTimeuuid(?)
    AND trade_id <= maxTimeuuid(?)
    ORDER BY trade_id DESC;
"""

# Q3.4
SELECT_TRADES_BY_ACCOUNT_STD = """
    SELECT * FROM trades_by_a_std
    WHERE account = ?
    AND trade_id >= minTimeuuid(?)
    AND trade_id <= maxTimeuuid(?)
    AND type = ?
    AND symbol = ?
    ORDER BY trade_id DESC;
"""

# Q3.5
SELECT_TRADES_BY_ACCOUNT_SD = """
    SELECT * FROM trades_by_a_sd
    WHERE account = ?
    AND trade_id >= minTimeuuid(?)
    AND trade_id <= maxTimeuuid(?)
    AND symbol = ?
    ORDER BY trade_id DESC;
"""
# <--------------------------------

# USER DATA AND INSTRUMENTS AVAILABLE
#-------------------------------->
USERS = [
    ('mike', 'Michael Jones'),
    ('stacy', 'Stacy Malibu'),
    ('john', 'John Doe'),
    ('marie', 'Marie Condo'),
    ('tom', 'Tomas Train')
]
INSTRUMENTS = [
    'ETSY', 'PINS', 'SE', 'SHOP', 'SQ', 'MELI', 'ISRG', 'DIS', 'BRK.A', 'AMZN',
    'VOO', 'VEA', 'VGT', 'VIG', 'MBB', 'QQQ', 'SPY', 'BSV', 'BND', 'MUB',
    'VSMPX', 'VFIAX', 'FXAIX', 'VTSAX', 'SPAXX', 'VMFXX', 'FDRXX', 'FGXX'
]

# <--------------------------------

# FUNCTIONS
#-------------------------------->
# Function to execute batch statements in Cassandra 10 at a time
def execute_batch(session,stmt, data): 
    batch_size = 10
    for i in range(0, len(data), batch_size):
        batch = BatchStatement()
        for item in data[i : i+batch_size]:
            batch.add(stmt, item)
        session.execute(batch)
    session.execute(batch)

# Function that generates data for the tables and inserts it into the database
def bulk_insert(session):
    acc_stmt = session.prepare("INSERT INTO accounts_by_user (username, account_number, cash_balance, name) VALUES (?, ?, ?, ?)")
    pos_stmt = session.prepare("INSERT INTO positions_by_account(account, symbol, quantity) VALUES (?, ?, ?)")
    tad_stmt = session.prepare("INSERT INTO trades_by_a_d (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    tatd_stmt = session.prepare("INSERT INTO trades_by_a_td (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    tastd_stmt = session.prepare("INSERT INTO trades_by_a_std (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    tasd_stmt = session.prepare("INSERT INTO trades_by_a_sd (account, trade_id, type, symbol, shares, price, amount) VALUES(?, ?, ?, ?, ?, ?, ?)")
    accounts = []

    accounts_num=10
    positions_by_account=100
    trades_by_account=1000
   
    # Generate accounts by user
    data = []
    for i in range(accounts_num):
        user = random.choice(USERS)
        account_number = str(uuid.uuid4())
        accounts.append(account_number)
        cash_balance = round(random.uniform(0.1, 100000.0), 2)
        data.append((user[0], account_number, cash_balance, user[1]))
    execute_batch(session, acc_stmt, data)
    
   
    # Genetate possitions by account
    acc_sym = {}
    data = []
    for i in range(positions_by_account):
        while True:
            acc = random.choice(accounts)
            sym = random.choice(INSTRUMENTS)
            if acc+'_'+sym not in acc_sym:
                acc_sym[acc+'_'+sym] = True
                quantity = random.randint(1, 500)
                data.append((acc, sym, quantity))
                break
    execute_batch(session, pos_stmt, data)

    # Generate trades by account
    data = []
    for i in range(trades_by_account):
        trade_id = random_date(datetime.datetime(2013, 1, 1), datetime.datetime(2022, 8, 31))
        acc = random.choice(accounts)
        sym = random.choice(INSTRUMENTS)
        trade_type = random.choice(['buy', 'sell'])
        shares = random.randint(1, 5000)
        price = round(random.uniform(0.1, 100000.0), 2)
        amount = shares * price
        data.append((acc, trade_id, trade_type, sym, shares, price, amount))
    execute_batch(session, tad_stmt, data)
    execute_batch(session, tatd_stmt, data)
    execute_batch(session, tastd_stmt, data)
    execute_batch(session, tasd_stmt, data)
    