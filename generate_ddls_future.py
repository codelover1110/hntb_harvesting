# Import Module
import os
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import csv
  
# Folder Path
path = "G:/2023_anthony/hntbtrade/HNTBTrade/Futures"
  
# Change the directory
os.chdir(path)
  
# Read text File
  
  
def read_text_file(file_path):
    with open(file_path, 'r') as f:
        print(f.read())


def connect_db():
    # Set up the connection parameters
    conn_params = {
        "host": "localhost",
        "port": "5432",
        "database": "hntb_1",
        "user": "postgres",
        "password": "12345",
        "sslmode": "prefer",
        "connect_timeout": "10"
    }

    # Connect to the database
    conn = psycopg2.connect(**conn_params)
    engine = create_engine(
        f'postgresql://{conn_params["user"]}:{conn_params["password"]}@{conn_params["host"]}:{conn_params["port"]}/{conn_params["database"]}')
    
    # Create a cursor object
    return conn.cursor(), engine


cursor, engine = connect_db()
symbol, exchange, market, category, currency, units, point, contract = None, None, None, None, None, None, None, None
# iterate through all file
for file in os.listdir():
    # Check whether file is in text format or not
    if file.endswith(".tsv"):
        file_path = f"{path}\{file}"
        table_name = file.split('.')
        specs = table_name[0]
        table_name = "futures_"+table_name[0]
        specs = specs.split('_')
        specs = specs[0]+".Specs.txt"
        specs_path = f"{path}\{specs}"
        
        with open(specs_path, 'r') as f:
            specs_data = str(f.read())
            specs_data = str(specs_data).splitlines()
            s = specs_data
            symbol = s[0].split(":")
            symbol = symbol[1].strip()
            exchange = s[1].split(":")
            exchange = exchange[1].strip()
            market = s[2].split(":")
            market = market[1].strip()
            category = s[3].split(":")
            category = category[1].strip()
            currency = s[4].split(":")
            currency = currency[1].strip()
            units = s[5].split(":")
            units = units[1].strip()
            point = s[6].split(":")
            point = point[1].strip()
            contract = s[7].split(":")
            contract = contract[1].strip()
        
        tsv_data = {}
        # call read text file function
        with open(file_path) as file:
            tsv_file = csv.reader(file, delimiter="\t")
            # date, o, h, l, c, v
            for line in tsv_file:
                dt_p = str(line[0])
                dt = dt_p[:4]+'-'+dt_p[4:6]+'-'+dt_p[6:]
                tsv_data['date'] = dt
                tsv_data['o'] = line[1]
                tsv_data['h'] = line[2]
                tsv_data['l'] = line[3]
                tsv_data['c'] = line[4]
                tsv_data['symbol'] = symbol
                data = pd.json_normalize(tsv_data)
                data.to_sql(table_name, engine, if_exists='append', index=False)