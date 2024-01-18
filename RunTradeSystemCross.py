# Import Module
import os
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import csv
from export_alls import export_tables

from configparser import ConfigParser

from datetime import date

today_date = date.today()
today = today_date.strftime("%Y-%m-%d")
  
configur = ConfigParser()
configur.read('config.ini')
VolumeIncrease = configur.getfloat('variables','volumeincrease')
TradingDays = configur.getfloat('variables','tradingdays')
MonthChange = configur.getfloat('variables','monthchange')
PriceChangeMin = configur.getfloat('variables','pricechangemin')
PriceChangeMax = configur.getfloat('variables','pricechangemax')
folder_path = configur.get('file_path', 'folder_path')

# DB info
database = configur.get('db', 'database')
user = configur.get('db', 'user')
password = configur.get('db', 'password')


pf_result = []
today_pf_result = []
futures_signals = []
stock_signals = []

def save_pf_result():
    with open(f'{folder_path}/result/logs/logs_signal.txt', 'w') as f:
        f.write('\n'.join(pf_result))

def save_today_pf_result():
    with open(f'{folder_path}/result/logs/today_logs_signal.txt', 'w') as f:
        if len(today_pf_result) > 0:
            f.write('\n'.join(today_pf_result))
        else:
            f.write('No Signals Today')

def connect_db():
    # Set up the connection parameters
    conn_params = {
        "host": "localhost",
        "port": "5432",
        "database": database,
        "user": user,
        "password": password,
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
    path = f"{folder_path}/Futures"
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
                    #try:
                    #    tsv_data['bRange'] = abs(float(line[1]) - float(line[4]))
                    #    tsv_data['bRange'] = round(tsv_data['bRange'], 4)
                    #except:
                    #    tsv_data['bRange'] = ''
                    #try:
                    #    tsv_data['direction'] = get_directions(float(line[1]),float(line[4]))
                    #except:
                    #    tsv_data['direction'] = ''
                    #try:
                    #    tsv_data['hammer'] = abs(float(line[2]) - float(line[4]))/abs(float(line[2]) - float(line[3]))
                    #    tsv_data['hammer'] = round(tsv_data['hammer'], 4)
                    #except:
                    #    tsv_data['hammer'] = ''
                    #print(tsv_data)
                    stock_d.append(tsv_data)
                    
            data = pd.json_normalize(stock_d)

            df = pd.DataFrame(data)

            df = df.sort_values('date')

            df['ema50'] = df['c'].ewm(span=50).mean()
            df['ema50'] = round(df['ema50'], 4)
            df['ema25'] = df['c'].ewm(span=25).mean()
            df['ema25'] = round(df['ema25'], 4)
            df['ema9'] = df['c'].ewm(span=9).mean()
            df['ema9'] = round(df['ema9'], 4)
            df['ema4'] = df['c'].ewm(span=4).mean()
            df['ema4'] = round(df['ema4'], 4)     
            df['min6'] = df['c'].rolling(window=6, min_periods=1).min()
            df['min6'] = round(df['min6'], 4)
            df['e50streak_under'] = 0
            df['e50streak_over'] = 0            

            type1 = [None, None]

            for i in range(2, len(df['o'])):

                if i < 3:
                    df['e50streak_under'][i] = 0
                    df['e50streak_over'][i] = 0
                elif df['ema50'][i-1] is not None and df['e50streak_under'][i-1] is not None:
                    if df['c'][i-1] < df['ema50'][i-1]:
                        df['e50streak_under'][i] = df['e50streak_under'][i-1] + 1
                        df['e50streak_over'][i] = 0 
                    elif df['c'][i-1] > df['ema50'][i-1]:
                        df['e50streak_under'][i] = 0 
                        df['e50streak_over'][i] = df['e50streak_over'][i-1] + 1


            ###################################   
            # Long Term System
            ###################################
                decision_where = None
                
                print(len(df['c']), '------------', (df['c']), i)

                # EntryPrice Calculation

                if df['c'][i-1] is not None and df['ema50'][i-1] is not None:
                    if (i >= 30 
                    and df['c'][i-1] < df['ema50'][i-1]
                    and df['c'][i] > df['ema50'][i]
                    and df['e50streak_under'][i] >= TradingDays
                    ):
                        type1.append('enterL1')
                        decision_where = 1.01
                    elif (i >= 30
                    and df['c'][i-1] > df['ema50'][i-1]
                    and df['c'][i] < df['ema50'][i]
                    and df['e50streak_over'][i] >= TradingDays
                    ):
                        type1.append('enterS1')
                        decision_where = 1.02


            ###################################   
            # Intermediate Term System
            ###################################

                    elif (
                    df['c'][i] > df['ema50'][i]
                    and df['ema4'][i] < df['ema9'][i] 
                    and df['c'][i-1] < df['ema9'][i-1]
                    and df['c'][i] > df['ema9'][i]
                    ):
                        type1.append('enterL2')
                        decision_where = 2.01
                    elif (
                    df['c'][i] < df['ema50'][i]
                    and df['ema4'][i] > df['ema9'][i] 
                    and df['c'][i-1] > df['ema9'][i-1]
                    and df['c'][i] < df['ema9'][i]
                    ):
                        type1.append('enterS2')
                        decision_where = 2.02


            ###################################   
            # Counter Trend System
            ###################################

                    elif i-6 > -1:
                        if (
                        df['c'][i-1] < df['ema9'][i-1]
                        and df['c'][i] > df['ema9'][i]
                        and df['c'][i-6] < df['ema9'][i-6]
                        and df['c'][i-5] < df['ema9'][i-5]
                        and df['c'][i-4] < df['ema9'][i-4]
                        and df['c'][i-3] < df['ema9'][i-3]
                        and df['c'][i-2] < df['ema9'][i-2]
                        ):
                            type1.append('enterL3')
                            decision_where = 3.01
                        elif (
                        df['c'][i-1] > df['ema9'][i-1]
                        and df['c'][i] < df['ema9'][i]
                        and i-6 > 0
                        and df['c'][i-6] > df['ema9'][i-6]
                        and df['c'][i-5] > df['ema9'][i-5]
                        and df['c'][i-4] > df['ema9'][i-4]
                        and df['c'][i-3] > df['ema9'][i-3]
                        and df['c'][i-2] > df['ema9'][i-2]
                        ):
                            type1.append('enterS3')
                            decision_where = 3.02
                
                if len(type1) == i:
                    type1.append(None)

                if  decision_where is not None and decision_where > 0:
                    pf_result.append(f"{df['date'][i]}, symbol: {table_name}, Trigger: {type1[i]}, close: {df['c'][i]}")
                    print(f"{table_name}: Trigger {type1}, datetime = {df['date'][i]}")         
                    if today == df['date'][i]:
                        today_pf_result.append(f"{df['date'][i]}, symbol: {table_name}, Trigger: {type1[i]}, close: {df['c'][i]}")
                        # today_pf_result.append(f"{table_name}: Trigger {type1}, close = {df['c'][i]}, datetime = {df['date'][i]}")    
                    print(len(type1), '---', i)       
                    futures_signals.append({
                        "symbol": symbol,
                        "date": df['date'][i],
                        "close": df['c'][i],
                        "type": type1[i],
                        "decision_number": decision_where
                        })


            df['decision_number'] = decision_where
            print(len(type1), '---- type1', len(df['o']))
            df['type1'] = type1


            data = df
            data.to_sql(table_name, engine, if_exists='replace', index=False)

    signals_df = pd.DataFrame.from_dict(futures_signals)
    signals_df.to_sql('signals_futures', engine, if_exists='replace', index=False)


def generate_ddls_stock():
    # Folder Path
    path = f"{folder_path}/Stock"
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
                    #try:
                    #    tsv_data['bRange'] = abs(float(line[1]) - float(line[4]))
                    #    tsv_data['bRange'] = round(tsv_data['bRange'], 4)
                    #except:
                    #    tsv_data['bRange'] = ''
                    #try:
                    #    tsv_data['direction'] = get_directions(float(line[1]),float(line[4]))
                    #except:
                    #    tsv_data['direction'] = ''
                    #try:
                    #    tsv_data['hammer'] = abs(float(line[2]) - float(line[4]))/abs(float(line[2]) - float(line[3]))
                    #    tsv_data['hammer'] = round(tsv_data['hammer'], 4)
                    #except:
                    #    tsv_data['hammer'] = ''
                    stock_d.append(tsv_data)

                    # print(tsv_data)

            data = pd.json_normalize(stock_d)

            df = pd.DataFrame(data)

            df = df.sort_values('date')

            df['ema50'] = df['c'].ewm(span=50).mean()
            df['ema50'] = round(df['ema50'], 4)
            df['ema25'] = df['c'].ewm(span=25).mean()
            df['ema25'] = round(df['ema25'], 4)
            df['ema9'] = df['c'].ewm(span=9).mean()
            df['ema9'] = round(df['ema9'], 4)
            df['ema4'] = df['c'].ewm(span=4).mean()
            df['ema4'] = round(df['ema4'], 4)     
            df['vol5ave'] = df['v'].rolling(window=5, min_periods=1).mean()
            df['vol5ave'] = round(df['vol5ave'], 4)
            df['min6'] = df['c'].rolling(window=6, min_periods=1).min()
            df['min6'] = round(df['min6'], 4)
            df['max20'] = df['c'].rolling(window=20, min_periods=1).max()
            df['max20'] = round(df['max20'], 4)
            df['min20'] = df['c'].rolling(window=20, min_periods=1).min()
            df['min20'] = round(df['min20'], 4)
            df['e50streak_under'] = 0
            df['e50streak_over'] = 0   

            type2 = [None, None]

            decision_number = [None, None]

            for i in range(2, len(df['o'])):

                if i < 3:
                    df['e50streak_under'][i] = 0
                    df['e50streak_over'][i] = 0
                elif df['ema50'][i-1] is not None and df['e50streak_under'][i-1] is not None:
                    if df['c'][i-1] < df['ema50'][i-1]:
                        df['e50streak_under'][i] = df['e50streak_under'][i-1] + 1
                        df['e50streak_over'][i] = 0 
                    elif df['c'][i-1] > df['ema50'][i-1]:
                        df['e50streak_under'][i] = 0 
                        df['e50streak_over'][i] = df['e50streak_over'][i-1] + 1


            ###################################   
            # Long Term System
            ###################################
                decision_where = None

                print(len(df['c']), '------------', (df['c']), i)

                # EntryPrice Calculation

                if df['c'][i-1] is not None and df['ema50'][i-1] is not None:
                    if (i >= 30 
                    and df['c'][i-1] < df['ema50'][i-1]
                    and df['c'][i] > df['ema50'][i]
                    and df['e50streak_under'][i] >= TradingDays
                    ):
                        type2.append('enterL1')
                        decision_where = 11.01
                    elif (i >= 30 
                    and df['c'][i-1] > df['ema50'][i-1]
                    and df['c'][i] < df['ema50'][i]
                    and df['e50streak_over'][i] >= TradingDays
                    ):
                        type2.append('enterS1')
                        decision_where = 11.02

            ###################################   
            # Intermediate Term System
            ###################################

                    elif (
                    df['c'][i] > df['ema50'][i]
                    and df['ema4'][i] < df['ema9'][i] 
                    and df['c'][i-1] < df['ema9'][i-1]
                    and df['c'][i] > df['ema9'][i]
                    and df['v'][i] > df['vol5ave'][i-1] * (1 + VolumeIncrease)
                    ):  
                        type2.append('enterL2')
                        decision_where = 12.01
                    elif (
                    df['c'][i] < df['ema50'][i]
                    and df['ema4'][i] > df['ema9'][i] 
                    and df['c'][i-1] > df['ema9'][i-1]
                    and df['c'][i] < df['ema9'][i]
                    and df['v'][i] > df['vol5ave'][i-1] * (1 + VolumeIncrease)
                    ):
                        type2.append('enterS2')
                        decision_where = 12.02



            ###################################   
            # Counter Trend System 3
            ###################################

                    elif i-6 > -1:
                        if (
                        df['c'][i-1] < df['ema9'][i-1]
                        and df['c'][i] > df['ema9'][i]
                        and df['c'][i-6] < df['ema9'][i-6]
                        and df['c'][i-5] < df['ema9'][i-5]
                        and df['c'][i-4] < df['ema9'][i-4]
                        and df['c'][i-3] < df['ema9'][i-3]
                        and df['c'][i-2] < df['ema9'][i-2]
                        and df['v'][i] > df['vol5ave'][i-1] * (1 + VolumeIncrease)
                        ):
                            type2.append('enterL3')
                            print(len(type2), '------------------ enterL3 -----', df['date'][i])
                            decision_where = 13.01

                        elif (
                        df['c'][i-1] > df['ema9'][i-1]
                        and df['c'][i] < df['ema9'][i]
                        and df['c'][i-6] > df['ema9'][i-6]
                        and df['c'][i-5] > df['ema9'][i-5]
                        and df['c'][i-4] > df['ema9'][i-4]
                        and df['c'][i-3] > df['ema9'][i-3]
                        and df['c'][i-2] > df['ema9'][i-2]
                        and df['v'][i] > df['vol5ave'][i-1] * (1 + VolumeIncrease)
                        ):
                            type2.append('enterS3')
                            print(len(type2), '------------------ enterS3 -----', df['date'][i])
                            decision_where = 13.02

            ###################################   
            # Counter Trend System 4
            ###################################

                        if i-25 > 0:
                            if (
                            (df['max20'][i-1] - df['c'][i-1]) / df['max20'][i-1] >= MonthChange
                            and (df['c'][i] / df['c'][i-1]) >= (1 + PriceChangeMin)
                            and (df['c'][i] / df['c'][i-1]) <= (1 + PriceChangeMax)
                            and df['v'][i] > df['vol5ave'][i-1] * (1 + VolumeIncrease)
                            ):
                                if len(type2) == i + 1 and type2[i] == 'enterL3':
                                    type2[i] = 'enterL3 enterL4'
                                else:
                                    type2.append('enterL4')
                                    print(len(type2), '------------------ enterL4 -----', df['date'][i])
                                decision_where = 14.01
                            elif (
                            (df['c'][i-1] - df['min20'][i-1]) / df['min20'][i-1] >= MonthChange
                            and (df['c'][i-1] / df['c'][i]) >= (1 + PriceChangeMin)
                            and (df['c'][i-1] / df['c'][i]) <= (1 + PriceChangeMax)
                            and df['v'][i] > df['vol5ave'][i-1] * (1 + VolumeIncrease)
                            ):
                                if len(type2) == i + 1 and type2[i] == 'enterS3':
                                    type2[i] = 'enterS3 enterS4'
                                else:
                                    type2.append('enterS4')
                                    print(len(type2), '------------------ enterS4 -----', df['date'][i])
                                decision_where = 14.02      
    

                if len(type2) == i:
                    type2.append(None)

                if decision_where is not None and decision_where > 10:
                    pf_result.append(f"{df['date'][i]}, symbol: {table_name}, Trigger: {type2[i]}, close: {df['c'][i]}")
                    # print(f"{table_name}: Trigger {type2}, datetime = {df['date'][i]}")         
                    if today == df['date'][i]:
                        today_pf_result.append(f"{df['date'][i]}, symbol: {table_name}, Trigger: {type2[i]}, close: {df['c'][i]}")
                        # today_pf_result.append(f"{table_name}: Trigger {type2}, close = {df['c'][i]}, datetime = {df['date'][i]}")           
                    stock_signals.append({
                        "symbol": symbol,
                        "date": df['date'][i],
                        "close": df['c'][i],
                        "type": type2[i],
                        "decision_number": decision_where
                        })


            df['decision_number'] = decision_where
            df['type2'] = type2

            data = df
            data.to_sql(table_name, engine, if_exists='replace', index=False)

    signals_df = pd.DataFrame.from_dict(stock_signals)
    signals_df.to_sql('signals_stock', engine, if_exists='replace', index=False)

            

if __name__ == '__main__':
    generate_ddls_future()
    generate_ddls_stock()
    save_pf_result()
    export_tables()
    save_today_pf_result()

