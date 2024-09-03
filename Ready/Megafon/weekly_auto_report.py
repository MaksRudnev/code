import cx_Oracle
import pandas as pd
import os
import sys
import shutil
import pyminizip as zp
import openpyxl
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, Date, String, Float, Numeric
import schedule
import time

usr = ""
pwd_dwx = ""

os.environ['ORACLE_HOME'] = ''
os.environ["NLS_LANG"] = "" 
dsnStr = cx_Oracle.makedsn(
    "", "", "").replace('SID', 'SERVICE_NAME')
engine_dwx = create_engine(
    '' % (usr, pwd_dwx, dsnStr))

sql_1 = f"""       
select /*+ parallel(10) */  
       *
from 
table
"""

def job():
    
    print('Started ', datetime.now())  
    
    df = pd.read_sql(sql_1, engine_dwx)
    
    df.to_excel('report.xlsx')
    inp = "./report.xlsx"
    oupt = "./report.zip"
    password = "pass"
    com_lvl = 0
    zp.compress(inp, None, oupt, password, com_lvl)    

    print('Commited ', datetime.now())   

job()

schedule.every().friday.at("14:15").do(job)

#Скрипт жил на tmux сессии на JupyterHub сервере, поэтому, по сути, работал вечно 
while True:
    schedule.run_pending()
    time.sleep(1)