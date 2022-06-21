import pandas as pd
import os
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

chunksize=100000

for itemCSV in os.listdir('C:\\Dados\\01 - Projeto Price\\CSV'):
    path = os.path.join(dir, itemCSV)

    if 'Dados' in itemCSV.title():
        print(itemCSV)
        colunas_dados =('coluna1','coluna2','coluna3')

        #truncate_table='TRUNCATE TABLE tb_dados'
        #engine.execute(truncate_table)

        idx = 1 
        for df in pd.read_csv(path,sep=';',encoding='ISO-8859-1', engine='python', chunksize=chunksize):
            
            #valida se a coluna do arquivo de entradar não existir na colunas_dadod remove a coluna 
            df = df.drop([x for x in df.columns if x not in colunas_dados], axis=1)
            for item in colunas_dados:
                if item not in df.columns:
                    df[item] = 0
            """ se o arquivo vier com menos coluna e criar a coluna faltante e preeche com zero
                    para que o arquivo fique no layout correto"""                 
            df.to_sql(name='tb_dados',con=engine,index=False ,if_exists='append',chunksize=chunksize)
            idx = idx+1
        print(f'total_linhas:' + str(chunksize * idx))