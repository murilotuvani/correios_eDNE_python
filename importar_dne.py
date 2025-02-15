import pandas as pd
import pymysql
from sqlalchemy import create_engine
import os

def salvar(df, table_name):
    sqlEngine = create_engine('mysql+pymysql://root:root@127.0.0.1:3388/cep', pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    transaction = dbConnection.begin()
    try:
        frame= df.to_sql(table_name, dbConnection, if_exists='replace')
        transaction.commit()
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("Table %s created successfully."%table_name);   
    finally:
        dbConnection.close()


def importa_logradouro(file):
    print(file)
    df = pd.read_csv(f'{folder}/{file}', sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_logradouro')

def importa_tabela(file):
    tabela = 'dne_' + file.replace('LOG_', '').replace('.txt', '').lower()
    print('File {}, table {}'.format(file, tabela))
    df = pd.read_csv(f'{folder}/{file}', sep='@', encoding='latin1')
    print(df.head())

folder = './tmp/correios/eDNE_Basico_25012/Delimitado'
# folder = '.'
files = os.listdir(folder)

for file in files:
    print(file)
    if file.startswith('LOG_'):
        if file.startswith('LOG_LOGRADOURO_'):
            importa_logradouro(file)
        else:
            importa_tabela(file)
    else:
        print('Not a LOG file {}'.format(file))
    # df = pd.read_csv(f'{folder}/{file}', sep='@', encoding='latin1')
    # print(df.head())
