#!/usr/bin/env python3
import logging
import os
import random
import platform
# import cassandra cluster from cassandra.cluster
from cassandra.cluster import Cluster

import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO') # Set log level to INFO
handler = logging.FileHandler('investments.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars releated to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', 'localhost')
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'investments')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def clear_screen():
    if platform.system() == 'Windows':
        os.system('cls')
    else:
        os.system('clear')

def print_menu():
    mm_options = {
        0: "Populate data",
        1: "Show accounts",
        2: "Show positions",
        3: "Show trade history",
        4: "Change username",
        5: "Exit",
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def print_trade_history_menu():
    clear_screen()
    thm_options = {
        1: "All Trades. (Optional date range. Defaults to latest 30 days)",
        2: "Trades by type (Buy or Sell). (Optional date range. Defaults to latest 30 days)",
        3: "Transaction by type (Buy or Sell) with instrument symbol. (Optional date range. Defaults to latest 30 days)",
        4: "Trades by symbol. (Optional date range. Defaults to latest 30 days)",
    }
    for key in thm_options.keys():
        print('    ', key, '--', thm_options[key])


def set_username():
    username = input('**** Username to use app: ')
    log.info(f"Username set to {username}")
    return username


def pick_account(accounts, account_index):
    clear_screen()
    print("Select Account:")
    for i, account in enumerate(accounts):
        print(i + 1, '--', account)
    account_index = int(input('Enter account index: '))
    return account_index - 1
        

def get_instrument_value(instrument):
    instr_mock_sum = sum(bytearray(instrument, encoding='utf-8'))
    return random.uniform(1.0, instr_mock_sum)
        

def option_2(session, username):
    accounts = []
    account_i = 0
    model.get_accounts(session, username, accounts)
    pick_account(accounts, account_i)
    model.get_positions_by_account(session, accounts[account_i])
    

def query_menu(session, option, username):
    clear_screen()
    if option == 1:
        q_option_1(session, username)
    if option == 2:
         q_option_2(session, username)
    if option == 3:
        q_option_3(session, username)
    if option == 4:
        q_option_4(session, username)


def q_option_1(session, username):
    accounts = []
    account_i = 0
    model.get_accounts(session, username, accounts)
    pick_account(accounts, account_i)
    model.get_trades_a_d(session, accounts[account_i])


def q_option_2(session, username):
    accounts = []
    account_i = 0
    model.get_accounts(session, username, accounts)
    pick_account(accounts, account_i)
    t_type = str(input('Enter trade type (buy or sell): ')).strip().lower()
    model.get_trades_a_d_range_type(session, accounts[account_i], t_type)
    input('Press enter to return to main menu ...')


def q_option_3(session, username):
    accounts = []
    account_i = 0
    model.get_accounts(session, username, accounts)
    pick_account(accounts, account_i)
    t_type = str(input('Enter trade type (buy or sell): ')).strip().lower()
    model.show_symbols()
    t_symbol = str(input('Enter instrument symbol (eg. VOO): ')).strip().upper()
    model.get_trades_a_std(session, accounts[account_i], t_type, t_symbol)
    input('Press enter to return to main menu ...')
    
    
def q_option_4(session, username):
    accounts = []
    account_i = 0
    model.get_accounts(session, username, accounts)
    pick_account(accounts, account_i)
    model.show_symbols()
    t_symbol = str(input('Enter instrument symbol (eg. VOO): ')).strip().upper()
    model.get_trades_a_sd(session, accounts[account_i], t_symbol)
    input('Press enter to return to main menu ...')

              
def main():
    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)  # USE investments;

    model.create_schema(session) # Create tables

    username = set_username()
    
    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        if option == 0:
            model.bulk_insert(session)
        if option == 1:
            model.get_user_accounts(session, username)
        if option == 2:
            option_2(session, username)
        if option == 3:
            print_trade_history_menu()
            tv_option = int(input('Enter your trade view choice: '))
            query_menu(session, tv_option, username)
            clear_screen()
        if option == 4:
            username = set_username()
        if option == 5:
            exit(0)


if __name__ == '__main__':
    main()
