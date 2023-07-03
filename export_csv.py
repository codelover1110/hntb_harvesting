from textwrap import indent
import colorama
from pandas.io.sql import pandasSQL_builder
from sqlalchemy import create_engine
# import pysftp
import pandas
from stat import S_ISDIR, S_ISREG
import os
import time
from datetime import datetime 
from datetime import timedelta
from colorama import Fore, Back
from sqlalchemy.sql import exists
from sqlalchemy.sql.coercions import expect
import requests
import pytz
from sqlalchemy.sql.expression import insert, table
colorama.init(strip=False, autoreset=True)
import glob
from dateutil import parser
import json


POSTGRES_USER = "postgres"
POSTGRES_PASS = "12345"
POSTGRES_IP = "localhost"
POSTGRES_PORT = "5432"
POSTGRES_DATABASE = "hntb_1"
engine_connection = None
engine = create_engine(f"postgresql://{POSTGRES_USER}:{POSTGRES_PASS}@{POSTGRES_IP}:{POSTGRES_PORT}/{POSTGRES_DATABASE}")




if __name__ == "__main__":

    query = 'SELECT * FROM public."futures_S2_B"'
    df = pandas.read_sql(query, engine)
    df.to_csv('futures_S2_B.csv', index=False)

