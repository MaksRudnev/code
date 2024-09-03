import cx_Oracle
import pandas as pd
import win32com.client
import os
import sys
sys.path.append(r'C:\Users\analysis')
from svd_libs.tools import *
cx_Oracle.init_oracle_client(lib_dir=r"C:\Users\instantclient_19_11")
import shutil
from datetime import datetime, timedelta

conn = get_connection_rep_b2b()
cursor = conn.cursor()
print('Connection successful')

cursor.execute("""truncate table rep_b2b.yndx_299_promo""")  

cursor.execute(f"""
insert into rep_b2b.yndx_299_promo
Select *
from table 
""")

cursor.execute("""commit""") 

print('Таблица rep_b2b.yndx_299_promo успешно обновлена')

shutil.copy(r'\\f_score\load_template_yndx_299_promo.xlsx', 
            r'\\dictionary_f_scoring')
print('File moved')

outlook = win32com.client.Dispatch('outlook.application')  
mail = outlook.CreateItem(0) 
mail.TO = 'maxim.rudnev@megafon.ru'
mail.Subject = 'ОБНОВЛЕНИЕ F-SCORE' 
mail.Body = """
Привет!

Обновленная модель отправлена в f-score:
- yndx_299_promo


Это уведомление сформировано автоматически, при обнаружении каких-либо проблем свяжитесь со мной.
""" 
mail.Send() 

print('Mail sent')


# In[ ]:




