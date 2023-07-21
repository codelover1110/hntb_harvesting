# Import Module
import os
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import csv
import numpy as np

pf_result = []

def save_pf_result():
    with open('G:/2023_anthony/hntbtrade/HNTBTrade/logs_signal.txt', 'w') as f:
        f.write('\n'.join(pf_result))

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


def get_directions(o,c):
    if c>o:
        return 'green'
    elif c<o:
        return 'red'
    else:
        return 'null'

def generate_ddls_stock():
    # Folder Path
    path = "G:/2023_anthony/hntbtrade/HNTBTrade/Stock"
    # Change the directory
    os.chdir(path)
  
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
            #print(table_name)
            # call read text file function
            with open(file_path) as file:
                tsv_file = csv.reader(file, delimiter="\t")
                # date, o, h, l, c, v
                for line in tsv_file:
                    tsv_data = {}
                    dt_p = str(line[0])
                    dt = dt_p[:4]+'-'+dt_p[4:6]+'-'+dt_p[6:]
                    tsv_data['symbol'] = symbol
                    tsv_data['date'] = dt
                    tsv_data['o'] = float(line[1])
                    tsv_data['h'] = float(line[2])
                    tsv_data['l'] = float(line[3])
                    tsv_data['c'] = float(line[4])
                    tsv_data['v'] = float(line[5])

                    # # Calculate Heikin Ashi Open, High, Low, and Close
                    # tsv_data['ha_o'] = (tsv_data['o']+ tsv_data['c']) / 2
                    # tsv_data['ha_h'] = tsv_data[['h', 'o', 'c']].max(axis=1)
                    # tsv_data['ha_c'] = (tsv_data['o'] + tsv_data['h'] + tsv_data['l'] + tsv_data['c']) / 4
                    # tsv_data['ha_l'] = tsv_data[['l', 'o', 'c']].min(axis=1)

                    # # try:
                    # #     tsv_data['bRange'] = abs(float(line[1]) - float(line[4]))
                    # #     tsv_data['bRange'] = round(tsv_data['bRange'], 4)
                    # # except:
                    # #     tsv_data['bRange'] = ''
                    # try:
                    #     tsv_data['direction'] = get_directions(tsv_data['ha_o'], tsv_data['ha_c'])
                    # except:
                    #     tsv_data['direction'] = ''

                    stock_d.append(tsv_data)

                    # print(tsv_data)

            data = pd.json_normalize(stock_d)
            df = pd.DataFrame(data)
            df = df.sort_values('date')

            # Calculate Heikin Ashi Open, High, Low, and Close
            df['ha_c'] = (df['o'] + df['h'] + df['l'] + df['c']) / 4
            df['ha_o'] = (df['o'].shift(1) + df['c'].shift(1)) / 2
            df.loc[0, 'ha_o'] = df.loc[0, 'o']  # Fix for the first bar
            df['ha_h'] = df[['h', 'ha_o', 'ha_c']].max(axis=1)
            df['ha_l'] = df[['l', 'ha_o', 'ha_c']].min(axis=1)

            # Round Heikin Ashi values to 4 decimal places
            df['ha_c'] = df['ha_c'].round(4)
            df['ha_o'] = df['ha_o'].round(4)
            df['ha_h'] = df['ha_h'].round(4)
            df['ha_l'] = df['ha_l'].round(4)


            df['ema9'] = df['ha_c'].ewm(span=9).mean()
            df['ema9'] = round(df['ema9'], 4)
            df['ema30'] = df['ha_c'].ewm(span=30).mean()
            df['ema30'] = round(df['ema30'], 4)
            df['delta_ema'] = df['ema9'] - df['ema30']
            df['delta_ema'] = round(df['delta_ema'], 4)

            eo = [0]
            endposition = ['']
            direction = ['']

            for i in range(1, len(df['delta_ema'])):
                ha_direction = get_directions(df['ha_o'][i], df['ha_c'][i])
                direction.append(ha_direction)
                # if delta_ema > 0 and Up trend
                if df['delta_ema'][i] > 0 and abs(df['delta_ema'][i]) > 0.01 and df['delta_ema'][i-1] < 0 and ha_direction == 'green':
                    eo.append(1)
                    endposition.append('entry')
                    continue

                # if delta_ema < 0 and Down trend
                elif df['delta_ema'][i] < 0 and abs(df['delta_ema'][i]) > 0.01 and df['delta_ema'][i-1] > 0 and ha_direction == 'red':
                    eo.append(1)
                    endposition.append('entry')
                    continue
                
                elif direction[i-1] == 'green' and ha_direction == 'red' and endposition[i-1] == 'entry':
                    eo.append(1)
                    endposition.append('exit')
                    continue
                
                elif direction[i-1] == 'red' and ha_direction == 'green' and  endposition[i-1] == 'entry':
                    eo.append(1)
                    endposition.append('exit')
                    continue
                else:
                    eo.append(0)
                    endposition.append(endposition[i-1])
                    continue

            df['direction'] = direction
            df['endposition'] = endposition
            df['eo'] = eo

            data = df
            data.to_sql(table_name, engine, if_exists='replace', index=False)


            


if __name__ == '__main__':
    generate_ddls_stock()
    save_pf_result()

