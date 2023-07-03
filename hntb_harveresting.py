# Import Module
import os
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import csv

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
        return 1
    elif c<o:
        return 0
    else:
        return 'null'

def generate_ddls_future():
    # Folder Path
    path = "G:/2023_anthony/hntbtrade/HNTBTrade/Futures"
    # Change the directory
    os.chdir(path)
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
            stock_d = []
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
            
            
            # call read text file function
            with open(file_path) as file:
                tsv_file = csv.reader(file, delimiter="\t")
                # date, o, h, l, c, v
                for line in tsv_file:
                    tsv_data = {}   
                    dt_p = str(line[0])
                    dt = dt_p[:4]+'-'+dt_p[4:6]+'-'+dt_p[6:]
                    tsv_data['date'] = dt
                    tsv_data['o'] = float(line[1])
                    tsv_data['h'] = float(line[2])
                    tsv_data['l'] = float(line[3])
                    tsv_data['c'] = float(line[4])
                    tsv_data['symbol'] = symbol
                    try:
                        tsv_data['tRange'] = abs(float(line[2]) - float(line[3]))
                        tsv_data['tRange'] = round(tsv_data['tRange'], 4)
                    except:
                        tsv_data['tRange'] = ''
                    try:
                        tsv_data['bRange'] = abs(float(line[1]) - float(line[4]))
                        tsv_data['bRange'] = round(tsv_data['bRange'], 4)
                    except:
                        tsv_data['bRange'] = ''
                    try:
                        tsv_data['direction'] = get_directions(float(line[1]),float(line[4]))
                    except:
                        tsv_data['direction'] = ''
                    try:
                        tsv_data['hammer'] = abs(float(line[2]) - float(line[4]))/abs(float(line[2]) - float(line[3]))
                        tsv_data['hammer'] = round(tsv_data['hammer'], 4)
                    except:
                        tsv_data['hammer'] = ''
                    #print(tsv_data)
                    stock_d.append(tsv_data)
                    
            data = pd.json_normalize(stock_d)

            df = pd.DataFrame(data)
            #print(df.head(50))
            df = df.sort_values('date')
            df['ema50'] = df['c'].ewm(span=50).mean()
            df['ema50'] = round(df['ema50'], 4)
            df['ema15'] = df['c'].ewm(span=15).mean()
            df['ema15'] = round(df['ema15'], 4)
            df['ch50'] = df['c'] - df['ema50']
            df['ch50'] = round(df['ch50'], 4)
            df['ch15'] = df['c'] - df['ema15']
            df['ch15'] = round(df['ch15'], 4)


            s15 = [1]
            for i in range(1, len(df['ch15'])):
                if df['ch15'][i] * df['ch15'][i-1] > 0:
                    s15.append(s15[i-1] + 1)
                else:
                    s15.append(1)
            df['s15'] = s15
            
            pf = []
            eo = []
            endposition = []
            c1_5 = []
            for i in range(0, len(df['direction'])):
                if i > 4 and df['c'][i-1] > df['c'][i-5]:
                    c1_5.append(1)
                else:
                    c1_5.append(0)

                if (df['direction'][i] == 1 
                    and df['c'][i] > df['ema50'][i] 
                    and i > 4
                    and df['c'][i-1] < df['c'][i-5]
                    and df['hammer'][i] <= 0.1):
                    pf_result.append(f"{table_name}: Trigger pf = 1, datetime = {df['date'][i]}")
                    print(f"{table_name}: Trigger pf = 1, datetime = {df['date'][i]}")
                    pf.append(1)

                    if endposition[i-1] == 'Short':
                        eo.append(1)
                        endposition.append("Long")
                    elif endposition[i-1] == '':
                        eo.append(1)
                        endposition.append("Long")
                    else:
                        eo.append('')
                        endposition.append(endposition[i-1])
                elif (df['direction'][i] == 0 
                    and df['c'][i] < df['ema50'][i] 
                    and i > 4
                    # and df['s15'][i-1] > 4
                    and df['c'][i-1] > df['c'][i-5]
                    and df['hammer'][i] >= 0.9):
                    pf_result.append(f"{table_name}: Trigger pf = 0, datetime = {df['date'][i]}")
                    print(f"{table_name}: Trigger pf = 0, datetime = {df['date'][i]}")
                    pf.append(0)

                    if endposition[i-1] == 'Long':
                        eo.append(1)
                        endposition.append("Short")
                    elif endposition[i-1] == '':
                        eo.append(1)
                        endposition.append("Short")
                    else:
                        eo.append('')
                        endposition.append(endposition[i-1])
                else:
                    pf.append('')
                    eo.append(0)
                    if len(endposition) == 0:
                        endposition.append('')
                    else:
                        endposition.append(endposition[i-1])
            
            df['pf'] = pf
            df['eo'] = eo
            df['endposition'] = endposition
            df['c1_5'] = c1_5

            data = df
            data.to_sql(table_name, engine, if_exists='replace', index=False)

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
                    tsv_data['date'] = dt
                    tsv_data['o'] = float(line[1])
                    tsv_data['h'] = float(line[2])
                    tsv_data['l'] = float(line[3])
                    tsv_data['c'] = float(line[4])
                    tsv_data['v'] = float(line[5])
                    tsv_data['symbol'] = symbol
                    try:
                        tsv_data['tRange'] = abs(float(line[2]) - float(line[3]))
                        tsv_data['tRange'] = round(tsv_data['tRange'], 4)
                    except:
                        tsv_data['tRange'] = ''
                    try:
                        tsv_data['bRange'] = abs(float(line[1]) - float(line[4]))
                        tsv_data['bRange'] = round(tsv_data['bRange'], 4)
                    except:
                        tsv_data['bRange'] = ''
                    try:
                        tsv_data['direction'] = get_directions(float(line[1]),float(line[4]))
                    except:
                        tsv_data['direction'] = ''
                    try:
                        tsv_data['hammer'] = abs(float(line[2]) - float(line[4]))/abs(float(line[2]) - float(line[3]))
                        tsv_data['hammer'] = round(tsv_data['hammer'], 4)
                    except:
                        tsv_data['hammer'] = ''
                    stock_d.append(tsv_data)

                    # print(tsv_data)

            data = pd.json_normalize(stock_d)
            df = pd.DataFrame(data)
            #print(df.head(50))
            df = df.sort_values('date')
            df['vol_ave_10'] = df['v'].rolling(window=10).mean()
            df['vol_ave_10'] = round(df['vol_ave_10'], 4)
            df['rVol'] = df['v'] / df['vol_ave_10'].iloc[-1]
            df['rVol'] = round(df['rVol'], 4)
            df['ema50'] = df['c'].ewm(span=50).mean()
            df['ema50'] = round(df['ema50'], 4)
            df['ema15'] = df['c'].ewm(span=15).mean()
            df['ema15'] = round(df['ema15'], 4)
            df['ch50'] = df['c'] - df['ema50']
            df['ch50'] = round(df['ch50'], 4)
            df['ch15'] = df['c'] - df['ema15']
            df['ch15'] = round(df['ch15'], 4)

            s15 = [1]
            for i in range(1, len(df['ch15'])):
                if df['ch15'][i] * df['ch15'][i-1] > 0:
                    s15.append(s15[i-1] + 1)
                else:
                    s15.append(1)
           
            df['s15'] = s15

            pf = []
            eo = []
            c1_5 = []
            endposition = []
            for i in range(0, len(df['direction'])):
                if i > 4 and df['c'][i-1] > df['c'][i-5]:
                    c1_5.append(1)
                else:
                    c1_5.append(0)
                if (df['c'][i] > 10
                    and df['v'][i] > 200000
                    and df['direction'][i] == 1
                    and df['c'][i] > df['ema50'][i]
                    and i > 4
                    and df['c'][i-1] < df['c'][i-5]
                    and df['hammer'][i] <= 0.1
                    and df['rVol'][i] > 1):
                    print(f"{table_name}: Trigger pf = 1, datetime = {df['date'][i]}")
                    pf_result.append(f"{table_name}: Trigger pf = 1, datetime = {df['date'][i]}")
                    pf.append(1)

                    if endposition[i-1] == 'Short':
                        eo.append(1)
                        endposition.append("Long")
                    elif endposition[i-1] == '':
                        eo.append(1)
                        endposition.append("Long")
                    else:
                        eo.append('')
                        endposition.append(endposition[i-1])
                    
                elif (df['direction'][i] == 0
                    and df['c'][i] < df['ema50'][i]
                    and i > 4
                    and df['c'][i-1] > df['c'][i-5]
                    and df['hammer'][i] >= 0.9
                    and df['rVol'][i] > 1):
                    print(f"{table_name}: Trigger pf = 1, datetime = {df['date'][i]}")
                    pf_result.append(f"{table_name}: Trigger pf = 1, datetime = {df['date'][i]}")
                    pf.append(0)

                    if endposition[i-1] == 'Long':
                        eo.append(1)
                        endposition.append("Short")
                    elif endposition[i-1] == '':
                        eo.append(1)
                        endposition.append("Short")
                    else:
                        eo.append('')
                        endposition.append(endposition[i-1])
                else:
                    pf.append('')
                    eo.append(0)
                    if len(endposition) == 0:
                        endposition.append('')
                    else:
                        endposition.append(endposition[i-1])

            df['pf'] = pf
            df['eo'] = eo
            df['endposition'] = endposition
            df['c1_5'] = c1_5

            data = df
            data.to_sql(table_name, engine, if_exists='replace', index=False)


            


if __name__ == '__main__':
    generate_ddls_future()
    generate_ddls_stock()
    save_pf_result()




# CalcuatedFields
# tRange = total range for the candle abs(high - low)
# bRange = body range abs(open - close)
# Direction = 0/1 (1 rising, 0 falling) (close > open = rising, close < open = falling, close = open = 'null')
# Hammer =  abs(high - close) / tRange (this will be a fraction from the top)
# VolAve10 = average volume of the last 10 candles
# rVol (Relative Volume) = volume of current candle / VolAve10
# e50 = ema50 line
# e15 = ema15 line
# ch50 = (greater or less than e50 line) = close - e50
# ch15 = (greater or less than e15 line) = close - e15
# s15 = (streak above or below e15 line) = if ch15 * candleprev[ch15] > 0, then increment 1, else restart at 1 (pretty much same as we do now on caats)