# Import Module
import os
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import csv
from export_alls import export_tables

from configparser import ConfigParser

from datetime import date

today = date.today()
  
configur = ConfigParser()
configur.read('config.ini')
ProfitTaking = configur.getfloat('variables','profittaking')
HammerBreak = configur.getfloat('variables','hammerbreak')
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
            df['tRange5'] = df['tRange'].rolling(window=5, min_periods=1).mean()
            df['tRange5'] = round(df['tRange5'], 4)
            df['max120'] = df['c'].rolling(window=120, min_periods=1).max()
            df['max120'] = round(df['max120'], 4)
            df['max50'] = df['c'].rolling(window=50, min_periods=1).max()
            df['max50'] = round(df['max50'], 4)
            df['max20'] = df['c'].rolling(window=20, min_periods=1).max()
            df['max20'] = round(df['max20'], 4)

            df['min120'] = df['c'].rolling(window=120, min_periods=1).min()
            df['min120'] = round(df['min120'], 4)
            df['min50'] = df['c'].rolling(window=50, min_periods=1).min()
            df['min50'] = round(df['min50'], 4)
            df['min20'] = df['c'].rolling(window=20, min_periods=1).min()
            df['min20'] = round(df['min20'], 4)

            df['ema25'] = df['c'].ewm(span=25).mean()
            df['ema25'] = round(df['ema25'], 4)
            df['ema50'] = df['c'].ewm(span=50).mean()
            df['ema50'] = round(df['ema50'], 4)
            df['ema75'] = df['c'].ewm(span=75).mean()
            df['ema75'] = round(df['ema75'], 4)
            df['ema100'] = df['c'].ewm(span=100).mean()
            df['ema100'] = round(df['ema100'], 4) 

            eo1 = [None, None]
            entryprice1 = [None, None]
            state1 = [None, None]
            type1 = [None, None]
            dayssince = [None, None]     

            state2 = [None, None]
            eo2 = [None, None]
            type2 = [None, None]
            stop2 = [None, None]
            entryprice2 = [None, None]    

            state3 = [None, None]
            eo3 = [None, None]
            type3 = [None, None]
            stop3 = [None, None]
            entryprice3 = [None, None]
            c1_c5 = [None, None]
            decision_number = [None, None]


            for i in range(2, len(df['o'])):
            ###################################   
            # (Type 1) – Long Term Trend
            ###################################
                decision_where = None
               
                # EntryPrice Calculation
                if eo1[i-1] == 'enterL1' or eo1[i-1] == 'enterS1':
                    entryprice1.append(df['o'][i-1])
                elif entryprice1[i-1] is not None:
                    entryprice1.append(entryprice1[i-1])
                else:
                    entryprice1.append(None)
                
                if i > 4:
                    c1_c5.append(round(df['c'][i-1] - df['c'][i-5], 4))
                else:
                    c1_c5.append(None)

                # First, check for LongEntry1
                if (state1[i-1] != 'Short1'
                    and df['c'][i-1] < (1-ProfitTaking)*df['max120'][i]
                    and df['c'][i] > df['max50'][i]):
                    type1.append('Long1')
                    # print("type1.append('Long1')", table_name)
                    if state1[i-1] != 'Long1':
                        eo1.append('enterL1')
                        state1.append('Long1')
                        pf_result.append(f"{table_name}: Trigger Type1 vs Long1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type1 vs Long1, datetime = {df['date'][i]}")
                        decision_where = 1
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type1 vs Long1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice1[i],
                            "type": type1[i],
                            "eo": eo1[i],
                            "state": state1[i],
                            # "stop": None,
                            "decision_number": decision_where
                        })
                    else:
                        eo1.append(None)
                        state1.append(state1[i-1])
                
                # Second Check for ShortEntry1
                elif (state1[i-1] != 'Long1'
                    and df['c'][i-1] > (1+ProfitTaking)*df['min120'][i]
                    and df['c'][i] < df['min50'][i]):
                    type1.append('Short1')
                    # print("type1.append('Short1')", table_name)
                    if state1[i-1] != 'Short1':
                        eo1.append('enterS1')
                        state1.append('Short1')
                        pf_result.append(f"{table_name}: Trigger Type1 vs Short1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print((f"{table_name}: Trigger Type1 vs Short1, datetime = {df['date'][i]}"))
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type1 vs Short1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        decision_where = 1.1
                        futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice1[i],
                            "type": type1[i],
                            "eo": eo1[i],
                            "state": state1[i],
                            # "stop": None,
                            "decision_number": decision_where
                        })
                    else:
                        eo1.append(None)
                        state1.append(state1[i-1])
                
                # Third, check for LongExit1
                elif state1[i-1] == 'Long1':
                    if (dayssince[i-1] > 100 and df['c'][i] < df['ema25'][i] - df['tRange5'][i]
                        or dayssince[i-1] > 75 and dayssince[i-1] <= 100 and df['c'][i] < df['ema50'][i] - df['tRange5'][i]
                        or dayssince[i-1] > 50 and dayssince[i-1] <= 75 and df['c'][i] < df['ema75'][i] - df['tRange5'][i]
                        or dayssince[i-1] <= 50 and df['c'][i] < df['ema100'][i] - df['tRange5'][i]):
                        type1.append('exitL1')
                        eo1.append('exitL1')
                        state1.append(None)
                        entryprice1[i] = None
                        pf_result.append(f"{table_name}: Trigger Type1 vs Long1 vs exitL1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print((f"{table_name}: Trigger Type1 vs Long1 vs exitL1, datetime = {df['date'][i]}"))
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type1 vs Long1 vs exitL1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        decision_where = 1.2
                        futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice1[i],
                            "type": type1[i],
                            "eo": eo1[i],
                            "state": state1[i],
                            # "stop": None,
                            "decision_number": decision_where
                        })
                    else:
                        type1.append(None)
                        eo1.append(None)
                        state1.append(state1[i-1])
                
                # Fourth, check for ShortExit1
                elif state1[i-1] == 'Short1':
                    if (dayssince[i-1] > 100 and df['c'][i] > df['ema25'][i] + df['tRange5'][i]
                        or dayssince[i-1] > 75 and dayssince[i-1] <= 100 and df['c'][i] > df['ema50'][i] + df['tRange5'][i]
                        or dayssince[i-1] > 50 and dayssince[i-1] <= 75 and df['c'][i] > df['ema75'][i] + df['tRange5'][i]
                        or dayssince[i-1] <= 50 and df['c'][i] > df['ema100'][i] + df['tRange5'][i]):
                        type1.append('exitL1')
                        eo1.append('exitL1')
                        state1.append(None)
                        entryprice1[i] = None
                        pf_result.append(f"{table_name}: Trigger Type1 vs Short1 vs exitL1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type1 vs Short1 vs exitL1, datetime = {df['date'][i]}")
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type1 vs Short1 vs exitL1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        decision_where = 1.3
                        futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice1[i],
                            "type": type1[i],
                            "eo": eo1[i],
                            "state": state1[i],
                            "decision_number": decision_where
                        })
                    else:
                        type1.append(None)
                        eo1.append(None)
                        state1.append(state1[i-1])
                else:
                    type1.append(None)
                    eo1.append(None)
                    state1.append(state1[i-1])
                
                if eo1[i] is not None:
                    dayssince.append(0)
                elif eo1[i-1] is not None:
                    dayssince.append(1)
                elif dayssince[i-1] is not None and dayssince[i-1] > 0:
                    dayssince.append(dayssince[i-1]+1)
                else:
                    dayssince.append(None)
                
                    
            ################################################
            # (Type 2) – Continuation Trend
            ################################################
               
                # EntryPrice Calculation
                if eo2[i-1] == 'enterL2' or eo2[i-1] == 'enterS2':
                    entryprice2.append(df['o'][i-1])
                elif entryprice2[i-1] is not None:
                    entryprice2.append(entryprice2[i-1])
                else:
                    entryprice2.append(None)

                # First, check for LongEntry2
                if (state2[i-1] != 'Short2'
                    and state2[i-1] != 'Long2'
                    and df['c'][i] > df['ema50'][i]
                    and i > 4
                    and df['c'][i-1] < df['c'][i-5]
                    and not isinstance(df['hammer'][i], str)
                    and df['hammer'][i] < HammerBreak
                    and df['direction'][i] == 0):
                    type2.append("Long2")
                    eo2.append('enterL2')
                    state2.append('Long2')
                    stop2.append(df['l'][i])

                    pf_result.append(f"{table_name}: Trigger Type2 vs enterL2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                    print(f"{table_name}: Trigger Type2 vs enterL2, datetime = {df['date'][i]}")
                    if today == df['date'][i]:
                        today_pf_result.append(f"{table_name}: Trigger Type2 vs enterL2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                    decision_where = 1.4
                    futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice2[i],
                            "type": type2[i],
                            "eo": eo2[i],
                            "state": state2[i],
                            "decision_number": decision_where
                        })

                    # print('type2.append("Long2")', table_name)

                # Second Check for ShortEntry2
                elif (state2[i-1] != 'Long2'
                    and state2[i-1] != 'Short2'
                    and df['c'][i] < df['ema50'][i]
                    and i > 4
                    and df['c'][i-1] < df['c'][i-5]
                    and not isinstance(df['hammer'][i], str)
                    and df['hammer'][i] < (1- HammerBreak)
                    and df['direction'][i] == 1):
                    type2.append("Short2")
                    eo2.append('enterS2')
                    state2.append('Short2')
                    stop2.append(df['h'][i])
                    # print('type2.append("Short2")', table_name)
                    pf_result.append(f"{table_name}: Trigger Type2 vs enterS2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                    print(f"{table_name}: Trigger Type2 vs enterS2, datetime = {df['date'][i]}")
                    if today == df['date'][i]:
                        today_pf_result.append(f"{table_name}: Trigger Type2 vs enterS2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                    decision_where = 1.5
                    futures_signals.append({
                        "symbol": symbol,
                        "date": df['date'][i],
                        "close": df['c'][i],
                        "entryprice": entryprice2[i],
                        "type": type2[i],
                        "eo": eo2[i],
                        "state": state2[i],
                        # "stop": stop2[i],
                        "decision_number": decision_where
                    })

                
                # Third, check for LongExit2
                elif state2[i-1] == 'Long2':

                    if (df['c'][i] > (1+(ProfitTaking/2)) * entryprice2[i]
                        and df['c'][i-1] < (1+(ProfitTaking/2)) * entryprice2[i]):
                        stop2.append(1+(ProfitTaking/4) * entryprice2[i])
                    elif (df['c'][i] >= (1+(ProfitTaking/4)) * entryprice2[i]
                        and df['c'][i-1] < (1+(ProfitTaking/4)) * entryprice2[i]):
                        stop2.append(df['o'][i])
                    else:
                        stop2.append(stop2[i-1])

                    if entryprice2[i] is not None and df['c'][i] > (1+(ProfitTaking*2)) * entryprice2[i]:
                        type2.append('exitL2')
                        eo2.append('exitL2')
                        state2.append(None)
                        stop2[i] = None
                        entryprice2[i] = None
                        pf_result.append(f"{table_name}: Trigger Type2 vs Long2 vs exitL2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type2 vs Long2 vs exitL2, datetime = {df['date'][i]}")
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type2 vs Long2 vs exitL2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        decision_where = 1.6
                        futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice2[i],
                            "type": type2[i],
                            "eo": eo2[i],
                            "state": state2[i],
                            "decision_number": decision_where
                        })
                    elif stop2[i] is not None and df['c'][i] < stop2[i]:
                        type2.append('exitStopL2')
                        eo2.append('exitL2')
                        state2.append(None)
                        stop2[i] = None
                        entryprice2[i] = None
                        pf_result.append(f"{table_name}: Trigger Type2 vs Long2 vs exitStopL2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type2 vs Long2 vs exitStopL2, datetime = {df['date'][i]}")
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type2 vs Long2 vs exitStopL2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        decision_where = 1.7
                        futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice2[i],
                            "type": type2[i],
                            "eo": eo2[i],
                            "state": state2[i],
                            "decision_number": decision_where
                        })
                    else:
                        type2.append(None)
                        eo2.append(None)
                        state2.append(state2[i-1])
                        
                    

                # Fourth, check for ShortExit2
                elif state2[i-1] == 'Short2':

                    if (df['c'][i] < (1-(ProfitTaking/2)) * entryprice2[i]
                        and df['c'][i-1] > (1-(ProfitTaking/2)) * entryprice2[i]):
                        stop2.append(1+(ProfitTaking/4) * entryprice2[i])
                    elif (df['c'][i] <= (1-(ProfitTaking/4)) * entryprice2[i]
                        and df['c'][i-1] > (1-(ProfitTaking/4)) * entryprice2[i]):
                        stop2.append(df['o'][i])
                    else:
                        stop2.append(stop2[i-1])

                    if len(entryprice2) > 0 and entryprice2[i] is not None and df['c'][i] < (1-ProfitTaking) * entryprice2[i]:
                        type2.append('exitS2')
                        eo2.append('exitS2')
                        state2.append(None)
                        pf_result.append(f"{table_name}: Trigger Type2 vs Short2 vs exitS2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type2 vs Short2 vs exitS2, datetime = {df['date'][i]}")
                        stop2[i] = None
                        entryprice2[i] = None

                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type2 vs Short2 vs exitS2, close = {df['c'][i]}, datetime = {df['date'][i]}")

                        decision_where = 1.8
                        futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice2[i],
                            "type": type2[i],
                            "eo": eo2[i],
                            "state": state2[i],
                            "decision_number": decision_where
                        })
                    elif stop2[i] is not None and df['c'][i] > stop2[i]:
                        type2.append('exitStopS2')
                        eo2.append('exitS2')
                        state2.append(None)
                        pf_result.append(f"{table_name}: Trigger Type2 vs Short2 vs exitStopS2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type2 vs Short2 vs exitStopS2, datetime = {df['date'][i]}")
                        stop2[i] = None
                        entryprice2[i] = None

                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type2 vs Short2 vs exitStopS2, close = {df['c'][i]}, datetime = {df['date'][i]}")

                        decision_where = 1.9
                        futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice2[i],
                            "type": type2[i],
                            "eo": eo2[i],
                            "state": state2[i],
                            "decision_number": decision_where
                        })

                    else:
                        type2.append(None)
                        eo2.append(None)
                        state2.append(state2[i-1])
                        
                        
                else:
                    type2.append(None)
                    eo2.append(None)
                    state2.append(state2[i-1])
                    if stop2[i-1] is not None:
                        stop2.append(stop2[i-1])
                    else:
                        stop2.append(None)
                

            #########################################################
            # (Type 3) - CounterTrend
            #########################################################
            
                # EntryPrice Calculation
                if eo3[i-1] == 'enterL3' or eo3[i-1] == 'enterS3':
                    entryprice3.append(df['o'][i-1])
                elif entryprice3[i-1] is not None:
                    entryprice3.append(entryprice3[i-1])
                else:
                    entryprice3.append(None)

                #First – determine if competing with Type 1 or Type 2
                if ((state1[i] == 'Long1' or state2[i] == 'Long2')
                    and state3[i-1] == 'Short3'):
                    eo3.append('exitS3')
                    type3.append('exitS3')
                    stop3.append(stop3[i-1])
                    state3.append(None)
                    pf_result.append(f"{table_name}: Trigger Type3 vs Short3 vs exitS3, close = {df['c'][i]}, datetime = {df['date'][i]}")
                    print(f"{table_name}: Trigger Type3 vs Short3 vs exitS3, datetime = {df['date'][i]}")
                    entryprice3[i] = None

                    if today == df['date'][i]:
                        today_pf_result.append(f"{table_name}: Trigger Type3 vs Short3 vs exitS3, close = {df['c'][i]}, datetime = {df['date'][i]}")

                    decision_where = 2.0
                    futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice3[i],
                            "type": type3[i],
                            "eo": eo3[i],
                            "state": state3[i],
                            "decision_number": decision_where
                        })
                    
                elif ((state1[i] == 'Short1' or state2[i] == 'Short2')
                    and state3[i-1] == 'Long3'):
                    eo3.append('exitL3')
                    type3.append('exitL3')
                    stop3.append(stop3[i-1])
                    state3.append(None)
                    pf_result.append(f"{table_name}: Trigger Type3 vs Long3 vs exitL3, close = {df['c'][i]}, datetime = {df['date'][i]}")
                    entryprice3[i] = None

                    if today == df['date'][i]:
                        today_pf_result.append(f"{table_name}: Trigger Type3 vs Long3 vs exitL3, close = {df['c'][i]}, datetime = {df['date'][i]}")

                    decision_where = 2.1
                    futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice3[i],
                            "type": type3[i],
                            "eo": eo3[i],
                            "state": state3[i],
                            "decision_number": decision_where
                        })
                
                # Second, check for LongEntry3
                elif (state3[i-1] != 'Long3'
                    and state3[i-1] != 'Short3'
                    and df['c'][i] < df['ema50'][i]
                    and df['c'][i-1] < (1 - (ProfitTaking/2)) * df['max20'][i]
                    and i > 4
                    and df['c'][i-1] < df['c'][i-5]
                    and not isinstance(df['hammer'][i], str)
                    and df['hammer'][i] <= HammerBreak
                    and df['tRange'][i] > (df['tRange'][i-1] + df['tRange'][i-2] + df['tRange'][i-3] + df['tRange'][i-4] + df['tRange'][i-5]) / 5):
                    type3.append('Long3')
                    eo3.append('enterL3')
                    state3.append('Long3')
                    stop3.append(df['l'][i])
                    pf_result.append(f"{table_name}: Trigger Type3 vs enterL3, close = {df['c'][i]}, datetime = {df['date'][i]}")
                    print(f"{table_name}: Trigger Type3 vs enterL3, datetime = {df['date'][i]}")

                    if today == df['date'][i]:
                        today_pf_result.append(f"{table_name}: Trigger Type3 vs enterL3, close = {df['c'][i]}, datetime = {df['date'][i]}")

                    decision_where = 2.2
                    futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice3[i],
                            "type": type3[i],
                            "eo": eo3[i],
                            "state": state3[i],
                            "decision_number": decision_where
                        })
                
                # Third Check for ShortEntry3
                elif (state3[i-1] != 'Long3'
                    and state3[i-1] != 'Short3'
                    and df['c'][i] > df['ema50'][i]
                    and df['c'][i-1] >= (1+ProfitTaking/2) * df['min20'][i]
                    and i > 4
                    and df['c'][i-1] > df['c'][i-5]
                    and not isinstance(df['hammer'][i], str)
                    and df['hammer'][i] >= (1-HammerBreak)
                    and df['tRange'][i] > (df['tRange'][i-1] + df['tRange'][i-2] + df['tRange'][i-3] + df['tRange'][i-4] + df['tRange'][i-5]) / 5):
                    type3.append('Short3')
                    eo3.append('enterS3')
                    state3.append('Short3')
                    stop3.append(df['l'][i])
                    pf_result.append(f"{table_name}: Trigger Type3 vs enterS3, close = {df['c'][i]}, datetime = {df['date'][i]}")
                    print(f"{table_name}: Trigger Type3 vs enterS3, datetime = {df['date'][i]}")

                    if today == df['date'][i]:
                        today_pf_result.append(f"{table_name}: Trigger Type3 vs enterS3, close = {df['c'][i]}, datetime = {df['date'][i]}")

                    decision_where = 2.3
                    futures_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice3[i],
                            "type": type3[i],
                            "eo": eo3[i],
                            "state": state3[i],
                            "decision_number": decision_where
                        })
                
                # Fouth, check for LongExit3
                elif state3[i-1] == 'Long3':
                    if (df['c'][i] >= (1+(ProfitTaking/4)) * entryprice3[i]
                        and df['c'][i-1] < (1+(ProfitTaking/4)) * entryprice3[i]):
                        stop3.append(entryprice3[i])
                    else:
                        stop3.append(stop3[i-1])

                    if entryprice3[i] is not None and df['c'][i] > (1+(ProfitTaking/2)) * entryprice3[i]:
                        type3.append('exitL3')
                        eo3.append('exitL3')
                        state3.append(None)
                        pf_result.append(f"{table_name}: Trigger Type3 vs exitL3, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type3 vs exitL3, datetime = {df['date'][i]}")
                        stop3[i] = None
                        entryprice3[i] = None

                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type3 vs exitL3, close = {df['c'][i]}, datetime = {df['date'][i]}")

                        decision_where = 2.4
                        futures_signals.append({
                                "symbol": symbol,
                                "date": df['date'][i],
                                "close": df['c'][i],
                                "entryprice": entryprice3[i],
                                "type": type3[i],
                                "eo": eo3[i],
                                "state": state3[i],
                                "decision_number": decision_where
                            })
                    elif stop3[i] is not None and df['c'][i] < stop3[i]:
                        type3.append('exitStopL3')
                        eo3.append('exitL3')
                        state3.append(None)
                        pf_result.append(f"{table_name}: Trigger Type3 vs exitStopL3, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type3 vs exitStopL3, datetime = {df['date'][i]}")
                        stop3[i] = None
                        entryprice3[i] = None

                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type3 vs exitStopL3, close = {df['c'][i]}, datetime = {df['date'][i]}")

                        decision_where = 2.5
                        futures_signals.append({
                                "symbol": symbol,
                                "date": df['date'][i],
                                "close": df['c'][i],
                                "entryprice": entryprice3[i],
                                "type": type3[i],
                                "eo": eo3[i],
                                "state": state3[i],
                                "decision_number": decision_where
                            })
                    else:
                        type3.append(None)
                        eo3.append(None)
                        state3.append(state3[i-1])
                
                
                # Fifth, check for ShortExit3
                elif state3[i-1] == 'Short3':
                    if (df['c'][i] <= (1-(ProfitTaking/4)) * entryprice3[i]
                        and df['c'][i-1] > (1-(ProfitTaking/4)) * entryprice3[i]):
                        stop3.append(entryprice3[i])
                    else:
                        stop3.append(stop3[i-1])

                    if entryprice3[i] is not None and df['c'][i-1] < (1 - (ProfitTaking/2)) * entryprice3[i]:
                        type3.append('exitS3')
                        eo3.append('exitS3')
                        state3.append(None)
                        pf_result.append(f"{table_name}: Trigger Type3 vs Short3 vs exitS3, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type3 vs Short3 vs exitS3, datetime = {df['date'][i]}")

                        stop3[i] = None
                        entryprice3[i] = None

                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type3 vs Short3 vs exitS3, close = {df['c'][i]}, datetime = {df['date'][i]}")

                        decision_where = 2.6
                        futures_signals.append({
                                "symbol": symbol,
                                "date": df['date'][i],
                                "close": df['c'][i],
                                "entryprice": entryprice3[i],
                                "type": type3[i],
                                "eo": eo3[i],
                                "state": state3[i],
                                # "stop": stop3[i],
                                "decision_number": decision_where
                            })
                    elif stop3[i] is not None and df['c'][i] > stop3[i]:
                        type3.append('exitStopS3')
                        eo3.append('exitS3')
                        state3.append(None)
                        pf_result.append(f"{table_name}: Trigger Type3 vs Short3 vs exitStopS3, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type3 vs Short3 vs exitStopS3, datetime = {df['date'][i]}")

                        stop3[i] = None
                        entryprice3[i] = None

                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type3 vs Short3 vs exitStopS3, close = {df['c'][i]}, datetime = {df['date'][i]}")

                        decision_where = 2.7
                        futures_signals.append({
                                "symbol": symbol,
                                "date": df['date'][i],
                                "close": df['c'][i],
                                "entryprice": entryprice3[i],
                                "type": type3[i],
                                "eo": eo3[i],
                                "state": state3[i],
                                "decision_number": decision_where
                            })
                    else:
                        type3.append(None)
                        eo3.append(None)
                        state3.append(state3[i-1])
                else:
                    type3.append(None)
                    eo3.append(None)
                    state3.append(state3[i-1])
                    stop3.append(stop3[i-1])
                
                decision_number.append(decision_where)

            df['decision_number'] = decision_number
            df['c1_c5'] = c1_c5
            df['entryprice1'] = entryprice1
            df['eo1'] = eo1
            df['state1'] = state1
            df['type1'] = type1
            df['dayssince'] = dayssince

            df['entryprice2'] = entryprice2
            df['stop2'] = stop2
            df['eo2'] = eo2
            df['state2'] = state2
            df['type2'] = type2

            df['entryprice3'] = entryprice3
            df['stop3'] = stop3
            df['eo3'] = eo3
            df['state3'] = state3
            df['type3'] = type3

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
            df['vol_ave_10'] = df['v'].rolling(window=10).mean()
            df['vol_ave_10'] = round(df['vol_ave_10'], 4)
            df['rVol'] = df['v'] / df['vol_ave_10'].iloc[-1]
            df['rVol'] = round(df['rVol'], 4)
            df['tRange5'] = df['tRange'].rolling(window=5, min_periods=1).mean()
            df['tRange5'] = round(df['tRange5'], 4)
            df['max120'] = df['c'].rolling(window=120, min_periods=1).max()
            df['max120'] = round(df['max120'], 4)
            df['max50'] = df['c'].rolling(window=50, min_periods=1).max()
            df['max50'] = round(df['max50'], 4)
            df['max20'] = df['c'].rolling(window=20, min_periods=1).max()
            df['max20'] = round(df['max20'], 4)

            df['min120'] = df['c'].rolling(window=120, min_periods=1).min()
            df['min120'] = round(df['min120'], 4)
            df['min50'] = df['c'].rolling(window=50, min_periods=1).min()
            df['min50'] = round(df['min50'], 4)
            df['min20'] = df['c'].rolling(window=20, min_periods=1).min()
            df['min20'] = round(df['min20'], 4)

            df['ema25'] = df['c'].ewm(span=25).mean()
            df['ema25'] = round(df['ema25'], 4)
            df['ema50'] = df['c'].ewm(span=50).mean()
            df['ema50'] = round(df['ema50'], 4)
            df['ema75'] = df['c'].ewm(span=75).mean()
            df['ema75'] = round(df['ema75'], 4)
            df['ema100'] = df['c'].ewm(span=100).mean()
            df['ema100'] = round(df['ema100'], 4) 

            eo1 = [None, None]
            entryprice1 = [None, None]
            state1 = [None, None]
            type1 = [None, None]
            dayssince = [None, None]     

            state2 = [None, None]
            eo2 = [None, None]
            type2 = [None, None]
            stop2 = [None, None]
            entryprice2 = [None, None]  
            c1_c5 = [None, None]  

            decision_number = [None, None]

            for i in range(2, len(df['o'])):
            ###################################   
            # (Type 1) – Long Term Trend
            ###################################

                decision_where = None
               
                # EntryPrice Calculation
                if eo1[i-1] == 'enterL1' or eo1[i-1] == 'enterS1':
                    entryprice1.append(df['o'][i-1])
                elif entryprice1[i-1] is not None:
                    entryprice1.append(entryprice1[i-1])
                else:
                    entryprice1.append(None)
                
                if i > 4:
                    c1_c5.append(round(df['c'][i-1] - df['c'][i-5], 4))
                else:
                    c1_c5.append(None)


                # First, check for LongEntry1
                if (state1[i-1] != 'Short1'
                    and df['c'][i-1] < (1-ProfitTaking)*df['max120'][i]
                    and df['c'][i] < df['max50'][i]):
                    type1.append('Long1')
                    # print("type1.append('Long1')", table_name)
                    if state1[i-1] != 'Long1':
                        eo1.append('enterL1')
                        state1.append('Long1')
                        pf_result.append(f"{table_name}: Trigger Type1 vs Long1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type1 vs Long1, datetime = {df['date'][i]}")

                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type1 vs Long1, close = {df['c'][i]}, datetime = {df['date'][i]}")

                        decision_where = 10
                        stock_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice1[i],
                            "type": type1[i],
                            "eo": eo1[i],
                            "state": state1[i],
                            # "stop": None,
                            "decision_number": decision_where
                        })
                    else:
                        eo1.append(None)
                        state1.append(state1[i-1])
                
                # Second Check for ShortEntry1
                elif (state1[i-1] != 'Long1'
                    and df['c'][i-1] > (1+ProfitTaking)*df['min120'][i]
                    and df['c'][i] > df['min50'][i]):
                    type1.append('Short1')
                    # print("type1.append('Short1')", table_name)
                    if state1[i-1] != 'Short1':
                        eo1.append('enterS1')
                        state1.append('Short1')
                        pf_result.append(f"{table_name}: Trigger Type1 vs Short1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print((f"{table_name}: Trigger Type1 vs Short1, datetime = {df['date'][i]}"))

                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type1 vs Short1, close = {df['c'][i]}, datetime = {df['date'][i]}")

                        decision_where = 10.1
                        stock_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice1[i],
                            "type": type1[i],
                            "eo": eo1[i],
                            "state": state1[i],
                            "decision_number": decision_where
                        })
                    else:
                        eo1.append(None)
                        state1.append(state1[i-1])
                
                # Third, check for LongExit1
                elif state1[i-1] == 'Long1':
                    if (dayssince[i-1] > 100 and df['c'][i] < df['ema25'][i] - df['tRange5'][i]
                        or dayssince[i-1] > 75 and dayssince[i-1] <= 100 and df['c'][i] < df['ema50'][i] - df['tRange5'][i]
                        or dayssince[i-1] > 50 and dayssince[i-1] <= 75 and df['c'][i] < df['ema75'][i] - df['tRange5'][i]
                        or dayssince[i-1] <= 50 and df['c'][i] < df['ema100'][i] - df['tRange5'][i]):
                        type1.append('exitL1')
                        eo1.append('exitL1')
                        state1.append(None)
                        entryprice1[i] = None
                        pf_result.append(f"{table_name}: Trigger Type1 vs Long1 vs exitL1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print((f"{table_name}: Trigger Type1 vs Long1 vs exitL1, datetime = {df['date'][i]}"))
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type1 vs Long1 vs exitL1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        decision_where = 10.2
                        stock_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice1[i],
                            "type": type1[i],
                            "eo": eo1[i],
                            "state": state1[i],
                            "decision_number": decision_where
                        })
                    else:
                        type1.append(None)
                        eo1.append(None)
                        state1.append(state1[i-1])
                
                # Fourth, check for ShortExit1
                elif state1[i-1] == 'Short1':
                    if (dayssince[i-1] > 100 and df['c'][i] > df['ema25'][i] + df['tRange5'][i]
                        or dayssince[i-1] > 75 and dayssince[i-1] <= 100 and df['c'][i] > df['ema50'][i] + df['tRange5'][i]
                        or dayssince[i-1] > 50 and dayssince[i-1] <= 75 and df['c'][i] > df['ema75'][i] + df['tRange5'][i]
                        or dayssince[i-1] <= 50 and df['c'][i] > df['ema100'][i] + df['tRange5'][i]):
                        type1.append('exitL1')
                        eo1.append('exitL1')
                        state1.append(None)
                        entryprice1[i] = None
                        pf_result.append(f"{table_name}: Trigger Type1 vs Short1 vs exitL1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type1 vs Short1 vs exitL1, datetime = {df['date'][i]}")
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type1 vs Short1 vs exitL1, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        decision_where = 10.3
                        stock_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice1[i],
                            "type": type1[i],
                            "eo": eo1[i],
                            "state": state1[i],
                            "decision_number": decision_where
                        })
                    else:
                        type1.append(None)
                        eo1.append(None)
                        state1.append(state1[i-1])
                else:
                    type1.append(None)
                    eo1.append(None)
                    state1.append(state1[i-1])
                
                if eo1[i] is not None:
                    dayssince.append(0)
                elif eo1[i-1] is not None:
                    dayssince.append(1)
                elif dayssince[i-1] is not None and dayssince[i-1] > 0:
                    dayssince.append(dayssince[i-1]+1)
                else:
                    dayssince.append(None)
                    
            ################################################
            # (Type 2) – Continuation Trend
            ################################################
               
                # EntryPrice Calculation
                if eo2[i-1] == 'enterL2' or eo2[i-1] == 'enterS2':
                    entryprice2.append(df['o'][i-1])
                elif entryprice2[i-1] is not None:
                    entryprice2.append(entryprice2[i-1])
                else:
                    entryprice2.append(None)
                # First, check for LongEntry2
                if (state2[i-1] != 'Short2'
                    and state2[i-1] != 'Long2'
                    and df['c'][i] > df['ema50'][i]
                    and i > 4
                    and df['c'][i-1] < df['c'][i-5]
                    and not isinstance(df['hammer'][i], str)
                    and df['hammer'][i] < HammerBreak
                    and df['direction'][i] == 0
                    and df['rVol'][i] > 1
                    and df['v'][i] > 100000):
                    type2.append("Long2")
                    eo2.append('enterL2')
                    state2.append('Long2')
                    stop2.append(df['l'][i])

                    pf_result.append(f"{table_name}: Trigger Type2 vs enterL2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                    print(f"{table_name}: Trigger Type2 vs enterL2, datetime = {df['date'][i]}")

                    if today == df['date'][i]:
                        today_pf_result.append(f"{table_name}: Trigger Type2 vs enterL2, close = {df['c'][i]}, datetime = {df['date'][i]}")

                    decision_where = 10.4
                    stock_signals.append({
                        "symbol": symbol,
                        "date": df['date'][i],
                        "close": df['c'][i],
                        "entryprice": entryprice2[i],
                        "type": type2[i],
                        "eo": eo2[i],
                        "state": state2[i],
                        "decision_number": decision_where
                    })


                # Second Check for ShortEntry2
                elif (state2[i-1] != 'Long2'
                    and state2[i-1] != 'Short2'
                    and df['c'][i] < df['ema50'][i]
                    and i > 4
                    and df['c'][i-1] < df['c'][i-5]
                    and not isinstance(df['hammer'][i], str)
                    and df['hammer'][i] < (1- HammerBreak)
                    and df['direction'][i] == 1
                    and df['rVol'][i] > 1
                    and df['v'][i] > 100000):
                    type2.append("Short2")
                    eo2.append('enterS2')
                    state2.append('Short2')
                    stop2.append(df['h'][i])

                    pf_result.append(f"{table_name}: Trigger Type2 vs enterS2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                    print(f"{table_name}: Trigger Type2 vs enterS2, datetime = {df['date'][i]}")

                    if today == df['date'][i]:
                        today_pf_result.append(f"{table_name}: Trigger Type2 vs enterS2, close = {df['c'][i]}, datetime = {df['date'][i]}")

                    decision_where = 10.5
                    stock_signals.append({
                        "symbol": symbol,
                        "date": df['date'][i],
                        "close": df['c'][i],
                        "entryprice": entryprice2[i],
                        "type": type2[i],
                        "eo": eo2[i],
                        "state": state2[i],
                        "decision_number": decision_where
                    })
                
                # Third, check for LongExit2
                elif state2[i-1] == 'Long2':
                    if (df['c'][i] > (1+(ProfitTaking/2)) * entryprice2[i]
                        and df['c'][i-1] < (1+(ProfitTaking/2)) * entryprice2[i]):
                        stop2.append(1+(ProfitTaking/4) * entryprice2[i])
                    elif (df['c'][i] >= (1+(ProfitTaking/4)) * entryprice2[i]
                        and df['c'][i-1] < (1+(ProfitTaking/4)) * entryprice2[i]):
                        stop2.append(df['o'][i])
                    else:
                        stop2.append(stop2[i-1])

                    if entryprice2[i] is not None and df['c'][i] > (1+ProfitTaking) * entryprice2[i]:
                        type2.append('exitL2')
                        eo2.append('exitL2')
                        state2.append(None)
                        pf_result.append(f"{table_name}: Trigger Type2 vs Long2 vs exitL2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type2 vs Long2 vs exitL2, datetime = {df['date'][i]}")
                        entryprice2[i] = None
                        stop2[i] = None
                        
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type2 vs Long2 vs exitL2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        
                        decision_where = 10.6
                        stock_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice2[i],
                            "type": type2[i],
                            "eo": eo2[i],
                            "state": state2[i],
                            "decision_number": decision_where
                        })
                    elif stop2[i] is not None and df['c'][i] < stop2[i]:
                        type2.append('exitStopL2')
                        eo2.append('exitL2')
                        state2.append(None)
                        pf_result.append(f"{table_name}: Trigger Type2 vs Long2 vs exitStopL2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type2 vs Long2 vs exitStopL2, datetime = {df['date'][i]}")
                        entryprice2[i] = None
                        stop2[i] = None
                        
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type2 vs Long2 vs exitStopL2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        
                        decision_where = 10.7
                        stock_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice2[i],
                            "type": type2[i],
                            "eo": eo2[i],
                            "state": state2[i],
                            "decision_number": decision_where
                        })

                    else:
                        type2.append(None)
                        eo2.append(None)
                        state2.append(state2[i-1])
                    

                # Fourth, check for ShortExit2
                elif state2[i-1] == 'Short2':
                    if (df['c'][i] < (1-(ProfitTaking/2)) * entryprice2[i]
                        and df['c'][i-1] > (1-(ProfitTaking/2)) * entryprice2[i]):
                        stop2.append(1+(ProfitTaking/4) * entryprice2[i])
                    elif (df['c'][i] <= (1-(ProfitTaking/4)) * entryprice2[i]
                        and df['c'][i-1] > (1-(ProfitTaking/4)) * entryprice2[i]):
                        stop2.append(df['o'][i])
                    else:
                        stop2.append(stop2[i-1])

                    if entryprice2[i] is not None and df['c'][i] < (1-ProfitTaking) * entryprice2[i]:
                        type2.append('exitS2')
                        eo2.append('exitS2')
                        state2.append(None)
                        pf_result.append(f"{table_name}: Trigger Type2 vs Short2 vs exitS2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type2 vs Short2 vs exitS2, datetime = {df['date'][i]}")
                        entryprice2[i] = None
                        stop2[i] = None
                        
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type2 vs Short2 vs exitS2, close = {df['c'][i]}, datetime = {df['date'][i]}")

                        decision_where = 10.8
                        stock_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice2[i],
                            "type": type2[i],
                            "eo": eo2[i],
                            "state": state2[i],
                            "decision_number": decision_where
                        })
                    elif stop2[i] is not None and df['c'][i] > stop2[i]:
                        type2.append('exitStopS2')
                        eo2.append('exitS2')
                        state2.append(None)
                        pf_result.append(f"{table_name}: Trigger Type2 vs Short2 vs exitStopS2, close = {df['c'][i]}, datetime = {df['date'][i]}")
                        print(f"{table_name}: Trigger Type2 vs Short2 vs exitStopS2, datetime = {df['date'][i]}")
                        entryprice2[i] = None
                        stop2[i] = None
                        
                        if today == df['date'][i]:
                            today_pf_result.append(f"{table_name}: Trigger Type2 vs Short2 vs exitStopS2, close = {df['c'][i]}, datetime = {df['date'][i]}")

                        decision_where = 10.9
                        stock_signals.append({
                            "symbol": symbol,
                            "date": df['date'][i],
                            "close": df['c'][i],
                            "entryprice": entryprice2[i],
                            "type": type2[i],
                            "eo": eo2[i],
                            "state": state2[i],
                            # "stop": stop2[i],
                            "decision_number": decision_where
                        })
                    else:
                        type2.append(None)
                        eo2.append(None)
                        state2.append(state2[i-1])
                    
                else:
                    type2.append(None)
                    eo2.append(None)
                    state2.append(state2[i-1])
                    if stop2[i-1] is not None:
                        stop2.append(stop2[i-1])
                    else:
                        stop2.append(None)
                
                decision_number.append(decision_where)
            
            df['decision_number'] = decision_number
            df['c1_c5'] = c1_c5
            df['entryprice1'] = entryprice1
            df['eo1'] = eo1
            df['state1'] = state1
            df['type1'] = type1
            df['dayssince'] = dayssince

            df['entryprice2'] = entryprice2
            df['stop2'] = stop2
            df['eo2'] = eo2
            df['state2'] = state2
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


