from sqlalchemy import INTEGER, create_engine, Integer, String
import pandas as pd
from urllib.parse import quote
import unicodedata
import re
import sqlalchemy

def salvar(df, table_name, if_exists='replace', dtype=None):
    sqlEngine = create_engine('mysql+pymysql://root:root@10.50.1.252:3306/ceps', pool_recycle=3600)
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


#UNIDADES OPERACIONAIS
arquivo1 = 'C:\\correios\\Delimitado\\DELTA_LOG_UNID_OPER.TXT'
dtype1 = {
        'uop_num': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_nu': sqlalchemy.INTEGER(),
        'log_nu': sqlalchemy.INTEGER(),
        'uop_no': sqlalchemy.types.VARCHAR(100),
        'uop_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'uop_in_cp': sqlalchemy.types.CHAR(1),
        'uop_no_abrev': sqlalchemy.types.VARCHAR(36),
        'uop_operacao': sqlalchemy.types.CHAR(3),
        'cep_ant': sqlalchemy.types.CHAR(8)
     }
dfunop = pd.read_csv(arquivo1, skiprows= 1,
                            sep= '@',
                            names= ['uop_num', 'ufe_sg', 'loc_nu', 'bai_nu', 'log_nu', 'uop_no', 'uop_endereco', 'cep', 'uop_in_cp', 'uop_no_abrev', 'uop_operacao', 'cep_ant'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfunop['arquivo'] = 25012
salvar(dfunop, 'dne_atualiza_unop', dtype= dtype1)

#GRANDES USUÁRIOS
arquivo2 = 'C:\\correios\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT'
dtype2 = {
        'gru_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_nu': sqlalchemy.INTEGER(),
        'log_nu': sqlalchemy.INTEGER(),
        'gru_no': sqlalchemy.types.VARCHAR(72),
        'gru_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'gru_no_abrev': sqlalchemy.types.VARCHAR(36),
        'gru_operacao': sqlalchemy.types.CHAR(3),
        'cep_ant': sqlalchemy.types.CHAR(8)
     }
dfgu = pd.read_csv(arquivo2, skiprows= 1,
                            sep= '@',
                            names= ['gru_nu', 'ufe_sg', 'loc_nu', 'bai_nu', 'log_nu', 'gru_no', 'gru_endereco', 'cep', 'gru_no_abrev', 'gru_operacao', 'cep_ant'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfgu['arquivo'] = 25012
salvar(dfgu, 'dne_atualiza_grandesusuarios', dtype= dtype2)

#BAIRROS
arquivo3 = 'C:\\correios\\Delimitado\\DELTA_LOG_BAIRRO.TXT'
dtype3 = {
        'bai_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_no': sqlalchemy.types.VARCHAR(72),
        'bai_no_abrev': sqlalchemy.types.VARCHAR(36),
        'bai_operacao': sqlalchemy.types.CHAR(3)
     }
dfb = pd.read_csv(arquivo3, skiprows= 1,
                            sep= '@',
                            names= ['bai_nu', 'ufe_sg', 'loc_nu', 'bai_no', 'bai_no_abrev', 'bai_operacao'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfb['arquivo'] = 25012
salvar(dfb, 'dne_atualiza_bairros', dtype= dtype3)

#CPC
arquivo4 = 'C:\\correios\\Delimitado\\DELTA_LOG_CPC.TXT'
dtype4 = {
        'cpc_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'cpc_no': sqlalchemy.types.VARCHAR(72),
        'cpc_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'cpc_operacao': sqlalchemy.types.CHAR(3),
        'cep_ant': sqlalchemy.types.CHAR(8)
     }
dfcpc = pd.read_csv(arquivo4, skiprows= 1,
                            sep= '@',
                            names= ['cpc_nu', 'ufe_sg', 'loc_nu', 'cpc_no', 'cpc_endereco', 'cep', 'cpc_operacao', 'cep_ant'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfcpc['arquivo'] = 25012
salvar(dfcpc, 'dne_atualiza_cpc', dtype= dtype4)

#FAIXA BAIRRO
arquivo5 = 'C:\\correios\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT'
dtype5 = {
        'bai_nu': sqlalchemy.INTEGER(),
        'fcb_cep_ini': sqlalchemy.types.CHAR(8),
        'fcb_cep_fim': sqlalchemy.types.CHAR(8),
        'fcb_operacao': sqlalchemy.types.CHAR(3)
     }
dffb = pd.read_csv(arquivo5, skiprows= 1,
                            sep= '@',
                            names= ['bai_nu', 'fcb_cep_ini', 'fcb_cep_fim', 'fcb_operacao'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dffb['arquivo'] = 25012
salvar(dffb, 'dne_atualiza_faixabairros', dtype= dtype5)


#FAIXA LOCALIDADES
arquivo6 = 'C:\\correios\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT'
dtype6 = {
        'loc_nu': sqlalchemy.INTEGER(),
        'loc_cep_ini': sqlalchemy.types.CHAR(8),
        'loc_cep_fim': sqlalchemy.types.CHAR(8),
        'loc_faixa_operacao': sqlalchemy.types.CHAR(3),
        'loc_tipo_faixa': sqlalchemy.types.CHAR(1)
     }
dffl = pd.read_csv(arquivo6, skiprows= 1,
                            sep= '@',
                            names= ['loc_nu', 'loc_cep_ini', 'loc_cep_fim', 'loc_faixa_operacao', 'loc_tipo_faixa'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dffl['arquivo'] = 25012
salvar(dffl, 'dne_atualiza_faixaloc', dtype= dtype6)

#FAIXA UOP
arquivo7 = 'C:\\correios\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT'
dtype7 = {
        'uop_nu': sqlalchemy.INTEGER(),
        'fnc_inicial': sqlalchemy.INTEGER(),
        'fnc_final': sqlalchemy.INTEGER(),
        'fnc_operacao': sqlalchemy.types.CHAR(3)
     }
dffuop = pd.read_csv(arquivo7, skiprows= 1,
                            sep= '@',
                            names= ['uop_nu', 'fnc_inicial', 'fnc_final', 'fnc_operacao'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dffuop['arquivo'] = 25012
salvar(dffuop, 'dne_atualiza_faixauop', dtype= dtype7)

#LOCALIDADE
arquivo8 = 'C:\\correios\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT'
dtype8 = {
        'loc_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_no': sqlalchemy.types.VARCHAR(72),
        'cep': sqlalchemy.types.CHAR(8),
        'loc_in_sit': sqlalchemy.types.CHAR(1),
        'loc_in_tipo_loc': sqlalchemy.types.CHAR(1),
        'loc_nu_sub': sqlalchemy.INTEGER(),
        'loc_no_abrev': sqlalchemy.types.VARCHAR(36),
        'mun_nu': sqlalchemy.types.CHAR(7),
        'loc_operacao': sqlalchemy.types.CHAR(3),
        'cep_ant': sqlalchemy.types.CHAR(8)
     }
dfl = pd.read_csv(arquivo8, skiprows= 1,
                            sep= '@',
                            names= ['loc_nu', 'ufe_sg', 'loc_no', 'cep', 'loc_in_sit', 'loc_in_tipo_loc', 'loc_nu_sub', 'loc_no_abrev', 'mun_nu', 'loc_operacao', 'cep_ant'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfl['arquivo'] = 25012
salvar(dfl, 'dne_atualiza_localidade', dtype= dtype8)

#LOGRADOURO
arquivo9 = 'C:\\correios\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT'
dtype9 = {
        'log_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_nu_ini': sqlalchemy.INTEGER(),
        'bai_num_fim': sqlalchemy.INTEGER(),
        'log_no': sqlalchemy.types.VARCHAR(100),
        'log_complemento': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'tlo_tx': sqlalchemy.types.VARCHAR(36),
        'log_sta_tlo': sqlalchemy.types.CHAR(1),
        'log_no_abrev': sqlalchemy.types.VARCHAR(36),
        'log_operacao': sqlalchemy.types.CHAR(3),
        'cep_ant': sqlalchemy.types.CHAR(8)
     }
dfl2 = pd.read_csv(arquivo9, skiprows= 1,
                            sep= '@',
                            names= ['log_nu', 'ufe_sg', 'loc_nu', 'bai_nu_ini', 'bai_num_fim', 'log_no', 'log_complemento', 'cep', 'tlo_tx', 'log_sta_tlo', 'log_no_abrev', 'log_operacao', 'cep_ant'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfl2['arquivo'] = 25012
salvar(dfl2, 'dne_atualiza_logradouro', dtype= dtype9)

#NUM SEC
arquivo10 = 'C:\\correios\\Delimitado\\DELTA_LOG_NUM_SEC.TXT'
dtype10 = {
        'log_nu': sqlalchemy.INTEGER(),
        'sec_nu_ini': sqlalchemy.VARCHAR(10),
        'sec_nu_fim': sqlalchemy.VARCHAR(10),
        'sec_in_lado': sqlalchemy.types.CHAR(1),
        'sec_operacao': sqlalchemy.types.CHAR(3)
     }
dfns = pd.read_csv(arquivo10, skiprows= 1,
                            sep= '@',
                            names= ['log_nu', 'sec_nu_ini', 'sec_nu_fim', 'sec_in_lado', 'sec_operacao'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfns['arquivo'] = 25012
salvar(dfns, 'dne_atualiza_numsec', dtype= dtype10)

#faixa cpc = sem dados