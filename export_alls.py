# Import Module
import os
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import csv
import pandas

POSTGRES_USER = "postgres"
POSTGRES_PASS = "12345"
POSTGRES_IP = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE = "hntb_1"
engine_connection = None
engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_IP}:{POSTGRES_PORT}/{POSTGRES_DATABASE}")

from configparser import ConfigParser
  
configur = ConfigParser()
configur.read('config.ini')
ProfitTaking = configur.getfloat('variables','profittaking')
HammerBreak = configur.getfloat('variables','hammerbreak')
folder_path = configur.get('file_path', 'folder_path')


def generate_ddls_future():
    # Folder Path
    path = f"{folder_path}/Futures"
    # Change the directory
    os.chdir(path)
    symbol, exchange, market, category, currency, units, point, contract = None, None, None, None, None, None, None, None
    # iterate through all file
    for file in os.listdir():
        # Check whether file is in text format or not
        if file.endswith(".tsv"):
            try:
                file_path = f"{path}\{file}"
                table_name = file.split('.')
                specs = table_name[0]
                table_name = "futures_"+specs.upper()

                query = f'SELECT * FROM public."{table_name}"'
                df = pandas.read_sql(query, engine)
                df.to_csv(f"../Result/tables/{table_name}.csv", index=False)
            except:

                pass
            

def generate_ddls_stock():
    # Folder Path
    path = f"{folder_path}/Stock"
    # Change the directory
    os.chdir(path)
  
    # iterate through all file
    for file in os.listdir():
        # Check whether file is in text format or not
        if file.endswith(".tsv"):
            try:
                file_path = f"{path}\{file}"
                table_name = file.split('.')
                specs = table_name[0]
                table_name = "stock_"+specs.upper()

                query = f'SELECT * FROM public."{table_name}"'
                print(query)
                df = pandas.read_sql(query, engine)
                print(df)
                df.to_csv(f"../Result/tables/{table_name}.csv", index=False)
            except:
                pass

def generate_signals():
    for table_name in ['signals_stock', 'signals_futures']:
        query = f'SELECT * FROM public."{table_name}"'
        print(query)
        df = pandas.read_sql(query, engine)
        df.to_csv(f"{folder_path}/Result/logs/{table_name}.csv", index=False)
            


if __name__ == '__main__':
    generate_ddls_future()
    generate_ddls_stock()
    generate_signals()

