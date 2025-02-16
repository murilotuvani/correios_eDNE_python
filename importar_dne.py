import pandas as pd
import pymysql
from sqlalchemy import create_engine
import os
import sqlalchemy

def salvar(df, table_name, if_exists='replace', dtype=None):
    sqlEngine = create_engine('mysql+pymysql://root:root@127.0.0.1:3388/cep', pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    transaction = dbConnection.begin()
    try:
        frame= df.to_sql(table_name, dbConnection, if_exists=if_exists, dtype=dtype)
        transaction.commit()
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("Table %s created successfully."%table_name);   
    finally:
        dbConnection.close()

'''
CAMPO	DESCRIÇÃO DO CAMPO	TIPO
LOG_NU	chave do logradouro	NUMBER(8)
SEPARADOR	@	
UFE_SG	sigla da UF	CHAR(2)
SEPARADOR	@	
LOC_NU	chave da localidade	NUMBER(8)
SEPARADOR	@	
BAI_NU_INI	chave do bairro inicial do logradouro 	NUMBER(8)
SEPARADOR	@	
BAI_NU_FIM	chave do bairro final do logradouro (opcional)	NUMBER(8)
SEPARADOR	@	
LOG_NO	nome do logradouro	VARCHAR2(100)
SEPARADOR	@	
LOG_COMPLEMENTO	complemento do logradouro (opcional)	VARCHAR2(100)
SEPARADOR	@	
CEP	CEP do logradouro	CHAR(8)
SEPARADOR	@	
TLO_TX	tipo de logradouro	VARCHAR2(36)
SEPARADOR	@	
LOG_STA_TLO	indicador de utilização do tipo de logradouro (S ou N) (opcional)	CHAR(1)
SEPARADOR	@	
LOG_NO_ABREV	abreviatura do nome do logradouro (opcional)	VARCHAR2(36)

'''
def importa_logradouro(file):
    colunas = ['log_nu','ufe_sg','loc_nu','bai_nu_ini','bai_nu_fim','log_no','log_complemento','cep','tlo_tx','log_sta_tlo','log_no_abrev']

    dtype = {
        'log_nu': sqlalchemy.types.NUMERIC(8),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.types.NUMERIC(8),
        'bai_nu_ini': sqlalchemy.types.NUMERIC(8),
        'bai_nu_fim': sqlalchemy.types.NUMERIC(8),
        'log_no': sqlalchemy.types.VARCHAR(100),
        'log_complemento': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'tlo_tx': sqlalchemy.types.VARCHAR(36),
        'log_sta_tlo': sqlalchemy.types.CHAR(1),
        'log_no_abrev': sqlalchemy.types.VARCHAR(36)
    }

    print(file)
    df = pd.read_csv(file, header=None, index_col='log_nu', names=colunas, sep='@', encoding='latin1')
    print(df.head())
    #, if_exists='append'
    salvar(df, 'dne_logradouro', if_exists='append', dtype=dtype)

def importa_tabela(file):
    tabela = 'dne_' + file.replace('LOG_', '').replace('.txt', '').lower()
    print('File {}, table {}'.format(file, tabela))
    df = pd.read_csv(f'{folder}/{file}', sep='@', encoding='latin1')
    print(df.head())

def importa_localidade(file):
    colunas = ['loc_nu','ufe_sg', 'loc_no','cep','loc_in_sit','loc_in_tipo_loc','loc_nu_sub','loc_no_abrev','mun_nu']

    dtype = {
        'loc_nu': sqlalchemy.types.NUMERIC(8),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_no': sqlalchemy.types.VARCHAR(72),
        'cep': sqlalchemy.types.CHAR(8),
        'loc_in_sit': sqlalchemy.types.CHAR(1),
        'loc_in_tipo_loc': sqlalchemy.types.CHAR(1),
        'loc_nu_sub': sqlalchemy.types.NUMERIC(8),
        'loc_no_abrev': sqlalchemy.types.VARCHAR(50),
        'mun_nu': sqlalchemy.types.CHAR(7)
    }

    df = pd.read_csv(file, header=None, index_col='loc_nu', names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_localidade', dtype=dtype)


def importa_localidade_variacao(file):
    colunas = ['loc_nu','val_nu', 'val_tx']

    dtype = {
        'loc_nu': sqlalchemy.types.NUMERIC(8),
        'val_nu': sqlalchemy.types.NUMERIC(8),
        'val_tx': sqlalchemy.types.VARCHAR(72)
    }

    df = pd.read_csv(file, header=None, index_col=['loc_nu','val_nu'], names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_localidade_variacao', dtype=dtype)

folder = './tmp/correios/eDNE_Basico_25012/Delimitado'
folder = r'C:\Users\MurilodeMoraesTuvani\tmp\correios\eDNE_Basico_25012\Delimitado'
# folder = '.'
files = os.listdir(folder)

for file in files:
    print(file)
    if file.casefold() == 'LOG_LOCALIDADE.TXT'.casefold():
        importa_localidade(os.path.join(folder, file))
    elif file.casefold() == 'LOG_VAR_LOC.TXT'.casefold():
        importa_localidade_variacao(os.path.join(folder, file))
    elif file.casefold().startswith('LOG_LOGRADOURO_'.casefold()):
        importa_logradouro(os.path.join(folder, file))
    else:
        print('Not a LOG file {}'.format(file))

