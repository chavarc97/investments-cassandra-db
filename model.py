#!/usr/bin/env python3
import datetime
import logging
import random
import uuid

import time_uuid
from cassandra.query import BatchStatement
from decimal import Decimal
from tabulate import tabulate

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
        type TEXT,
        trade_id TIMEUUID,
        symbol TEXT,
        shares DECIMAL,
        price DECIMAL,
        amount DECIMAL,
        PRIMARY KEY ((account), type, trade_id)
    ) WITH CLUSTERING ORDER BY (type ASC,trade_id DESC);
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
        PRIMARY KEY ((account), type, symbol, trade_id)
    ) WITH CLUSTERING ORDER BY (type ASC, symbol ASC, trade_id DESC)
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
        PRIMARY KEY ((account), symbol, trade_id)
    ) WITH CLUSTERING ORDER BY (symbol ASC, trade_id DESC)
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
"""

# Find all trades for an account, with optional date range, 
# Query 1
SELECT_TRADES_BY_ACCOUNT_DATE = """
    SELECT toDate(trade_id), type, symbol, shares, price, amount 
    FROM trades_by_a_d WHERE account = ? 
    ORDER BY trade_id DESC
    LIMIT 10
"""

# Qeury 1.1
SELECT_TRADES_BY_ACCOUNT_DATE_RANGE = """
    SELECT toDate(trade_id) as trade_date, type, symbol, shares, price, amount 
    FROM trades_by_a_d
    WHERE account = ?
    AND toTimestamp(trade_id) >= toTimestamp(now()) - 2592000000  
    AND toTimestamp(trade_id) <= toTimestamp(now())
"""

# "Trades by type (Buy or Sell). (Optional date range. Defaults to latest 30 days)",
# Query 2
SELECT_TRADES_BY_ACCOUNT_DATE_RANGE_TYPE = """
    SELECT toDate(trade_id), type, symbol, shares, price, amount
    FROM trades_by_a_td 
    WHERE account = ? 
    AND type = ?
    LIMIT 10
"""

# find all trades in an account, transaction type and symbol, ordered by trade_id
# Query 3
SELECT_TRADES_BY_ACCOUNT_STD = """
    SELECT toDate(trade_id) as trade_date, type, symbol, shares, price, amount 
    FROM trades_by_a_std
    WHERE account = ?
    AND type = ?
    AND symbol = ?
    LIMIT 10
"""

# "Trades by symbol. (Optional date range. Defaults to latest 30 days)"
# Query 4
SELECT_TRADES_BY_ACCOUNT_SD = """
    SELECT toDate(trade_id) as trade_date, type, symbol, shares, price, amount 
    FROM trades_by_a_sd
    WHERE account = ?
    AND symbol = ?
    LIMIT 10
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
        cash_balance = Decimal(random.uniform(1000.0, 100000.0)).quantize(Decimal('0.01'))
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
        price = Decimal(random.uniform(1000.0, 100000.0)).quantize(Decimal('0.01'))
        amount = shares * price
        data.append((acc, trade_id, trade_type, sym, shares, price, amount))
    execute_batch(session, tad_stmt, data)
    execute_batch(session, tatd_stmt, data)
    execute_batch(session, tastd_stmt, data)
    execute_batch(session, tasd_stmt, data)


def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    rand_date = start_date + datetime.timedelta(days=random_number_of_days)
    return time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(rand_date))


def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))
   
    
def create_schema(session):
    log.info("Creating model schema")
    session.execute(CREATE_USERS_TABLE)
    session.execute(CREATE_POSSITIONS_BY_ACCOUNT_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_DATE_TABLE)
    session.execute(CREATE_TRADES_BY_ACCOUNT_TD_TABLE)
    session.execute(CREATE_TABLE_BY_ACCOUNT_STD_TABLE)
    session.execute(CREATE_TABLE_BY_ACCOUNT_SD_TABLE)
    

def show_symbols():
    print("Available symbols: ")
    for sym in INSTRUMENTS:
        print("- ", sym)


# Function to execute the queries
"""
This function executes the queries to retrieve the accounts and store them in a list.
"""
def get_accounts(session, username, accounts = []):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    for row in rows:
        accounts.append(row.account_number)
    return accounts


def get_user_accounts(session, username):
    log.info(f"Retrieving {username} accounts")
    stmt = session.prepare(SELECT_USER_ACCOUNTS)
    rows = session.execute(stmt, [username])
    rows_list = list(rows)
    formatted_rows = [
        (row.account_number, row.name, row.cash_balance)
        for row in rows_list
    ]
    headers = ["Account Number", "Name", "Cash Balance"]
    table = tabulate(formatted_rows, headers= headers, tablefmt="pretty")
    print(table)
        
        
def get_positions_by_account(session, account):
    log.info(f"Retrieving positions for account: {account}")
    stmt = session.prepare(SELECT_POSITIONS_BY_ACCOUNT)
    rows = session.execute(stmt, [account])
    rows_list = list(rows)
    formatted_rows = [
        (row.symbol, row.quantity)
        for row in rows_list
    ]
    headers = ["Symbol", "Quantity"]
    table = tabulate(formatted_rows, headers= headers, tablefmt="pretty")
    print(table)
        

def get_trades_a_d(session, account):
    log.info(f"Retrieving trades for account: {account} by date")
    stmt = session.prepare(SELECT_TRADES_BY_ACCOUNT_DATE)
    rows = session.execute(stmt, [account])
     # Convert rows to a list to make sure we can iterate over them
    rows_list = list(rows)
    formatted_rows = [
        (
            row[0],  # Access the first column (toDate(trade_id))
            row.type, 
            row.symbol, 
            row.shares, 
            f"{float(row.price):,.2f}",  # Convert Decimal to float and format
            f"{float(row.amount):,.2f}"  # Convert Decimal to float and format
        )
        for row in rows_list
    ]
    headers = ["Trade Date", "Type", "Symbol", "Shares", "Price", "Amount"]
    table = tabulate(formatted_rows, headers= headers, tablefmt="pretty")
    print(table)     
        

def get_trades_a_d_range_type(session, account, type, start_date=None, end_date=None):
    log.info(f"Retrieving trades for account: {account} by date range and type")
    stmt = session.prepare(SELECT_TRADES_BY_ACCOUNT_DATE_RANGE_TYPE)
    rows = session.execute(stmt, [account, type])
     # Convert rows to a list to make sure we can iterate over them
    rows_list = list(rows)
    formatted_rows = [
        (
            row[0],  # Access the first column (toDate(trade_id))
            row.type, 
            row.symbol, 
            row.shares, 
            f"{float(row.price):,.2f}",  # Convert Decimal to float and format
            f"{float(row.amount):,.2f}"  # Convert Decimal to float and format
        )
        for row in rows_list
    ]
    headers = ["Trade Date", "Type", "Symbol", "Shares", "Price", "Amount"]
    table = tabulate(formatted_rows, headers= headers, tablefmt="pretty")
    print(table)
        

def get_trades_a_std(session, account, t_type, t_symbol):
    log.info(f"Retrieving trades for account: {account} by date, type and symbol")
    stmt = session.prepare(SELECT_TRADES_BY_ACCOUNT_STD)
    rows = session.execute(stmt, [account, t_type, t_symbol])
    rows_list = list(rows)
    formatted_rows = [
        (
            row[0],  # Access the first column (toDate(trade_id))
            row.type, 
            row.symbol, 
            row.shares, 
            f"{float(row.price):,.2f}",  # Convert Decimal to float and format
            f"{float(row.amount):,.2f}"  # Convert Decimal to float and format
        )
        for row in rows_list
    ]
    headers = ["Trade Date", "Type", "Symbol", "Shares", "Price", "Amount"]
    table = tabulate(formatted_rows, headers= headers, tablefmt="pretty")
    print(table)
        
        
def get_trades_a_sd(session, account, t_symbol):
    log.info(f"Retrieving trades for account: {account} by date and symbol")
    stmt = session.prepare(SELECT_TRADES_BY_ACCOUNT_SD)
    rows = session.execute(stmt, [account, t_symbol])
    rows_list = list(rows)
    formatted_rows = [
        (
            row[0],  # Access the first column (toDate(trade_id))
            row.type, 
            row.symbol, 
            row.shares, 
            f"{float(row.price):,.2f}",  # Convert Decimal to float and format
            f"{float(row.amount):,.2f}"  # Convert Decimal to float and format
        )
        for row in rows_list
    ]
    headers = ["Trade Date", "Type", "Symbol", "Shares", "Price", "Amount"]
    table = tabulate(formatted_rows, headers= headers, tablefmt="pretty")
    print(table)
        