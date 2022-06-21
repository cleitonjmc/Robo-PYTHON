#import cx_Oracle utilizado para querys no oracle
import pandas as pd
import os, shutil
from pathlib import Path
from os.path import getmtime
from datetime import datetime
from sqlalchemy import create_engine
from decouple import config
MYSQL_USER=config('MYSQL_USER')
MYSQL_PASS=config('MYSQL_PASS')
MYSQL_HOST=config('MYSQL_HOST')
MYSQL_PORT=config('MYSQL_PORT')
MYSQL_BASE=config('MYSQL_BASE')

def handleEngine():
    engine =create_engine(f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_BASE}")
    return engine

engine = handleEngine()


for item in Path("C:\\Dados").glob("*.txt"):
    print(item)

    diretorio = Path("C:\\Dados").glob("*.txt")
    arquivo = min(diretorio, key=getmtime)
    
    arq0 = arquivo.as_posix()

    if ".txt" in arq0:
        arq1 = arq0[27:]
        arq2 = arq0[27:-11]
        data = arq0[-10:-4] 

        print(data)
        
        #Arquivo com as datas para serem inseridas na query
        df = pd.read_excel("C:\\Dados\\datas.xlsx")
        df = df[(df['ANOMES']==int(data))]
        print(df)

        dt1 = df['ANOMES']
        dt2 = df['FIRSTDATE']
        dt3 = df['LASTDATE']
        dt4 = df['FIRSTDATE_1']
        dt5 = df['LASTDATE_1']
        

        print("============================================================")
        print(f"-- INICIO EXTRACAO ANALITICO : {arq1}")
        print("-- =========================================================")

        file_old = os.path.join(f"C:\\Dados\\files\\{arq1}")
        file_new = os.path.join(f"C:\\Dados\\files\\{arq2}_{data}_LOAD.ini")

        os.rename(file_old,file_new)       

        with open(f"C:\\Dados\\query\\{arq2}.sql", encoding='UTF-8') as f:
            sql_query_string = f.read();
            query = sql_query_string.replace('&1',f'{int(dt1)}').replace('&2',f'{int(dt2)}').replace('&3',f'{int(dt3)}').replace('&4',f'{dt4}').replace('&5',f'{dt5}')                
            print(query)
            sql_query = pd.read_sql_query(query, engine)

            sql_query.to_csv(f"C:\\Dados\\files\\{arq2}_{data}.csv",sep=';',encoding='ISO-8859-1', index=None)   
 
        try:
            shutil.rmtree(f"C:\\Dados\\files\\{arq2}_{data}_LOAD.ini")
        except OSError:
            os.remove(f"C:\\Dados\\files\\{arq2}_{data}_LOAD.ini")

    engine.close()

    print("============================================================")
    print(f"-- EXTRACAO CONCLUIDA : {arq1}")
    print("-- =========================================================")


