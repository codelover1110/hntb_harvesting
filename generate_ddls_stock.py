# Import Module
import os
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import csv
  
# Folder Path
path = "G:/2023_anthony/hntbtrade/HNTBTrade/Stock"
  
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


# iterate through all file
for file in os.listdir():
    # Check whether file is in text format or not
    if file.endswith(".tsv"):
        file_path = f"{path}\{file}"
        table_name = file.split('.')
        symbol = table_name[0]
        table_name = "stock_"+table_name[0]
        stock_d = []        
        print(table_name)
        # call read text file function
        with open(file_path) as file:
            tsv_file = csv.reader(file, delimiter="\t")
            # date, o, h, l, c, v
            for line in tsv_file:
                tsv_data = {}
                dt_p = str(line[0])
                dt = dt_p[:4]+'-'+dt_p[4:6]+'-'+dt_p[6:]
                tsv_data['date'] = dt
                tsv_data['o'] = line[1]
                tsv_data['h'] = line[2]
                tsv_data['l'] = line[3]
                tsv_data['c'] = line[4]
                tsv_data['v'] = line[5]
                tsv_data['symbol'] = symbol
                stock_d.append(tsv_data)

        data = pd.json_normalize(stock_d)
        data.to_sql(table_name, engine, if_exists='replace', index=False)