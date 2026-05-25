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
arquivos_x= []
colunas_x= []
dtype_x= {} 
versoes_x= []
def importar_dne (arquivos, colunas, tabela, dtype, versoes):
    dfs= []
    for arquivo, versao in zip (arquivos, versoes):
        df= pd.read_csv(arquivo,
                sep= '@',
                names= colunas,
                header= None,
                encoding= 'ISO-8859-1',
                )
        df['atualizacao']= versao
        dfs.append(df)
    if len(dfs)== 1:
        df_final= dfs[0]
    else:
        df_final= pd.concat(dfs,
                    ignore_index= True
                )
    df_final = df_final.drop_duplicates()
    salvar(df_final, tabela, dtype= dtype)


#UNIDADES OPERACIONAIS
arquivos_unidopd= [
        'c:\\projetos\\eDNE\\25012\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25021\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25022\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25031\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25032\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25041\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25042\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25051\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25052\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25061\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25062\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25071\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25072\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25081\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25082\\Delimitado\\DELTA_LOG_UNID_OPER.TXT',
        'c:\\projetos\\eDNE\\25091\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25092\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25101\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25102\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25111\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25112\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25121\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\25122\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\26011\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\26012\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\26021\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\26022\\Delimitado\\DELTA_LOG_UNID_OPER.TXT', 
        'c:\\projetos\\eDNE\\26031\\Delimitado\\DELTA_LOG_UNID_OPER.TXT'
]
colunas_unidopd= ['uop_num', 'ufe_sg', 'loc_nu', 'bai_nu', 'log_nu', 'uop_no', 'uop_endereco', 'cep', 'uop_in_cp', 'uop_no_abrev', 'uop_operacao', 'cep_ant']
dtype_unidopd= {
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
versoes_unidopd= [25012, 25021, 25022, 25031, 25032, 25041, 25042, 25051, 25052, 25061, 25062, 25071, 25072, 25081, 25082, 25091, 25092, 25101, 25102, 25111, 25112, 25121, 25122, 26011, 26012, 26021, 26022, 26031]
importar_dne(arquivos_unidopd, colunas_unidopd, 'dne_delta_uop', dtype_unidopd, versoes_unidopd)

#BAIRROS
arquivos_bairrosd= [
        'c:\\projetos\\eDNE\\25031\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25032\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25041\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25042\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25051\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25052\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25061\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25062\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25071\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25072\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25081\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25082\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25012\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25021\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25022\\Delimitado\\DELTA_LOG_BAIRRO.TXT',
        'c:\\projetos\\eDNE\\25091\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25092\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25101\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25102\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25111\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25112\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25121\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\25122\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\26011\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\26012\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\26021\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\26022\\Delimitado\\DELTA_LOG_BAIRRO.TXT', 
        'c:\\projetos\\eDNE\\26031\\Delimitado\\DELTA_LOG_BAIRRO.TXT'
]
colunas_bairrosd= ['bai_nu', 'ufe_sg', 'loc_nu', 'bai_no', 'bai_no_abrev', 'bai_operacao']
dtype_bairrosd= {
        'bai_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_no': sqlalchemy.types.VARCHAR(72),
        'bai_no_abrev': sqlalchemy.types.VARCHAR(36),
        'bai_operacao': sqlalchemy.types.CHAR(3)
     }
versoes_bairrosd= [25031, 25032, 25041, 25042, 25051, 25052, 25061, 25062, 25071, 25072, 25081, 25082, 25012, 25021, 25022, 25091, 25092, 25101, 25102, 25111, 25112, 25121, 25122, 26011, 26012, 26021, 26022, 26031]
importar_dne(arquivos_bairrosd, colunas_bairrosd, 'dne_delta_bairro', dtype_bairrosd, versoes_bairrosd)

#CPC
arquivos_cpcd= [
        'c:\\projetos\\eDNE\\25012\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25021\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25022\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25031\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25032\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25041\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25042\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25051\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25052\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25061\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25062\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25071\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25072\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25081\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25082\\Delimitado\\DELTA_LOG_CPC.TXT',
        'c:\\projetos\\eDNE\\25091\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25092\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25101\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25102\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25111\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25112\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25121\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\25122\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\26011\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\26012\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\26021\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\26022\\Delimitado\\DELTA_LOG_CPC.TXT', 
        'c:\\projetos\\eDNE\\26031\\Delimitado\\DELTA_LOG_CPC.TXT'
]
colunas_cpcd= ['cpc_nu', 'ufe_sg', 'loc_nu', 'cpc_no', 'cpc_endereco', 'cep', 'cpc_operacao', 'cep_ant']
dtype_cpcd= {'cpc_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'cpc_no': sqlalchemy.types.VARCHAR(72),
        'cpc_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'cpc_operacao': sqlalchemy.types.CHAR(3),
        'cep_ant': sqlalchemy.types.CHAR(8)
     }
versoes_cpcd= [25012, 25021, 25022, 25031, 25032, 25041, 25042, 25051, 25052, 25061, 25062, 25071, 25072, 25081, 25082, 25091, 25092, 25101, 25102, 25111, 25112, 25121, 25122, 26011, 26012, 26021, 26022, 26031]
importar_dne(arquivos_cpcd, colunas_cpcd, 'dne_delta_cpc', dtype_cpcd, versoes_cpcd)

#FAIXA BAIRRO
arquivos_faixabaid= [
        'c:\\projetos\\eDNE\\25012\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25021\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25022\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25031\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25032\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25041\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25042\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25051\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25052\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25061\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25062\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25071\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25072\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25081\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25082\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT',
        'c:\\projetos\\eDNE\\25091\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25092\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25101\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25102\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25111\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25112\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25121\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\25122\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\26011\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\26012\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\26021\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\26022\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT', 
        'c:\\projetos\\eDNE\\26031\\Delimitado\\DELTA_LOG_FAIXA_BAI.TXT'
]
colunas_faixabaid= ['bai_nu', 'fcb_cep_ini', 'fcb_cep_fim', 'fcb_operacao']
dtype_faixabaid= {'bai_nu': sqlalchemy.INTEGER(),
        'fcb_cep_ini': sqlalchemy.types.CHAR(8),
        'fcb_cep_fim': sqlalchemy.types.CHAR(8),
        'fcb_operacao': sqlalchemy.types.CHAR(3)
     }
versoes_faixabaid= [25012, 25021, 25022, 25031, 25032, 25041, 25042, 25051, 25052, 25061, 25062, 25071, 25072, 25081, 25082, 25091, 25092, 25101, 25102, 25111, 25112, 25121, 25122, 26011, 26012, 26021, 26022, 26031]
importar_dne(arquivos_faixabaid, colunas_faixabaid, 'dne_delta_faixa_bairro', dtype_faixabaid, versoes_faixabaid)

#FAIXA LOCALIDADE
arquivos_faixalocd= [
        'c:\\projetos\\eDNE\\25012\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25021\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25022\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25031\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25032\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25041\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25042\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25051\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25052\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25061\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25062\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25071\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25072\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25081\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25082\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25091\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25092\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25101\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25102\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25111\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25112\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25121\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\25122\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\26011\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\26012\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\26021\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\26022\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT',
        'c:\\projetos\\eDNE\\26031\\Delimitado\\DELTA_LOG_FAIXA_LOC.TXT'
]
colunas_faixalocd= ['loc_nu', 'loc_cep_ini', 'loc_cep_fim', 'loc_faixa_operacao', 'loc_tipo_faixa']
dtype_faixalocd= {'loc_nu': sqlalchemy.INTEGER(),
        'loc_cep_ini': sqlalchemy.types.CHAR(8),
        'loc_cep_fim': sqlalchemy.types.CHAR(8),
        'loc_faixa_operacao': sqlalchemy.types.CHAR(3),
        'loc_tipo_faixa': sqlalchemy.types.CHAR(1)
     }
versoes_faixalocd= [25012, 25021, 25022, 25031, 25032, 25041, 25042, 25051, 25052, 25061, 25062, 25071, 25072, 25081, 25082, 25091, 25092, 25101, 25102, 25111, 25112, 25121, 25122, 26011, 26012, 26021, 26022, 26031]
importar_dne(arquivos_faixalocd, colunas_faixalocd, 'dne_delta_faixa_loc', dtype_faixalocd, versoes_faixalocd)

#FAIXA UOP
arquivos_faixauopd= ['c:\\projetos\\eDNE\\25012\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25021\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25022\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25031\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25032\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25041\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25042\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25051\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25052\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25061\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25062\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25071\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25072\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25081\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25082\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT',
                     'c:\\projetos\\eDNE\\25091\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25092\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25101\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25102\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25111\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25112\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25121\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\25122\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\26011\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\26012\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\26021\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\26022\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT', 
                     'c:\\projetos\\eDNE\\26031\\Delimitado\\DELTA_LOG_FAIXA_UOP.TXT'
                     ]
colunas_faixauopd= ['uop_nu', 'fnc_inicial', 'fnc_final', 'fnc_operacao']
dtype_faixauopd= {'uop_nu': sqlalchemy.INTEGER(),        
                'fnc_inicial': sqlalchemy.INTEGER(),
                'fnc_final': sqlalchemy.INTEGER(),
                'fnc_operacao': sqlalchemy.types.CHAR(3)
     }
versoes_faixauopd= [25012, 25021, 25022, 25031, 25032, 25041, 25042, 25051, 25052, 25061, 25062, 25071, 25072, 25081, 25082, 25091, 25092, 25101, 25102, 25111, 25112, 25121, 25122, 26011, 26012, 26021, 26022, 26031]
importar_dne(arquivos_faixauopd, colunas_faixauopd, 'dne_delta_faixa_uop', dtype_faixauopd, versoes_faixauopd)

#LOCALIDADE
arquivos_locd= ['c:\\projetos\\eDNE\\25031\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25032\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25041\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25042\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25051\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25052\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25061\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25062\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25071\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25072\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25081\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25082\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25012\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25021\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25022\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT',
                     'c:\\projetos\\eDNE\\25091\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25092\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25101\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25102\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25111\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25112\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25121\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\25122\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\26011\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\26012\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\26021\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\26022\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT', 
                     'c:\\projetos\\eDNE\\26031\\Delimitado\\DELTA_LOG_LOCALIDADE.TXT'
                     ]
colunas_locd= ['loc_nu', 'ufe_sg', 'loc_no', 'cep', 'loc_in_sit', 'loc_in_tipo_loc', 'loc_nu_sub', 'loc_no_abrev', 'mun_nu', 'loc_operacao', 'cep_ant']
dtype_locd= {'loc_nu': sqlalchemy.INTEGER(),        
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
versoes_locd= [25031, 25032, 25041, 25042, 25051, 25052, 25061, 25062, 25071, 25072, 25081, 25082, 25012, 25021, 25022, 25091, 25092, 25101, 25102, 25111, 25112, 25121, 25122, 26011, 26012, 26021, 26022, 26031]
importar_dne(arquivos_locd, colunas_locd, 'dne_delta_localidade', dtype_locd, versoes_locd)

#LOGRADOURO
arquivos_logradourod= ['c:\\projetos\\eDNE\\25012\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25021\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25022\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25031\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25032\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25041\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25042\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25051\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25052\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25061\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25062\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25071\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25072\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25081\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25082\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT',
                       'c:\\projetos\\eDNE\\25091\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25092\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25101\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25102\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25111\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25112\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25121\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\25122\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\26011\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\26012\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\26021\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\26022\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT', 
                       'c:\\projetos\\eDNE\\26031\\Delimitado\\DELTA_LOG_LOGRADOURO.TXT'
                       ]
colunas_logradourod= ['log_nu', 'ufe_sg', 'loc_nu', 'bai_nu_ini', 'bai_num_fim', 'log_no', 'log_complemento', 'cep', 'tlo_tx', 'log_sta_tlo', 'log_no_abrev', 'log_operacao', 'cep_ant']
dtype_logradourod= {'log_nu': sqlalchemy.INTEGER(),
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
versoes_logradourod= [25082, 25081, 25072, 25071, 25062, 25061, 25052, 25051, 25042, 25041, 25032, 25031, 25022, 25021, 25012, 26031, 26022, 26021, 26012, 26011, 25122, 25121, 25112, 25111, 25102, 25101, 25092, 25091]
importar_dne(arquivos_logradourod, colunas_logradourod, 'dne_delta_logradouro', dtype_logradourod, versoes_logradourod)

#NUM SEC
arquivos_numsecd= ['c:\\projetos\\eDNE\\25012\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25021\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25022\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25031\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25032\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25041\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25042\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25051\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25052\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25061\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25062\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25071\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25072\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25081\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25082\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25091\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25092\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25101\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25102\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25111\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25112\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25121\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\25122\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\26011\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\26012\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\26021\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\26022\\Delimitado\\DELTA_LOG_NUM_SEC.TXT', 
                   'c:\\projetos\\eDNE\\26031\\Delimitado\\DELTA_LOG_NUM_SEC.TXT']
colunas_numsecd= ['log_nu', 'sec_nu_ini', 'sec_nu_fim', 'sec_in_lado', 'sec_operacao']
dtype_numsecd= {'log_nu': sqlalchemy.INTEGER(),
                'sec_nu_ini': sqlalchemy.VARCHAR(10),
                'sec_nu_fim': sqlalchemy.VARCHAR(10),
                'sec_in_lado': sqlalchemy.types.CHAR(1),
                'sec_operacao': sqlalchemy.types.CHAR(3)
             }
versoes_numsecd= [25082, 25081, 25072, 25071, 25062, 25061, 25052, 25051, 25042, 25041, 25032, 25031, 25022, 25021, 25012, 26031, 26022, 26021, 26012, 26011, 25122, 25121, 25112, 25111, 25102, 25101, 25092, 25091]
importar_dne(arquivos_numsecd, colunas_numsecd, 'dne_delta_num_sec', dtype_numsecd, versoes_numsecd)

#GRANDES USUÁRIOS
arquivos_gud = ['c:\\projetos\\eDNE\\25012\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25021\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25022\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25031\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25032\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25041\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25042\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25051\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25052\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25061\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25062\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25071\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25072\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25081\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25082\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT',
               'c:\\projetos\\eDNE\\25091\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25092\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25101\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25102\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25111\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25112\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25121\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\25122\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\26011\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\26012\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\26021\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\26022\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT', 
               'c:\\projetos\\eDNE\\26031\\Delimitado\\DELTA_LOG_GRANDE_USUARIO.TXT']
colunas_gud = ['gru_nu', 'ufe_sg', 'loc_nu', 'bai_nu', 'log_nu', 'gru_no', 'gru_endereco', 'cep', 'gru_no_abrev', 'gru_operacao', 'cep_ant']
dtype_gud= {
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
        'cep_ant': sqlalchemy.types.CHAR(8),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_gud= [25012, 25021, 25022, 25031, 25032, 25041, 25042, 25051, 25052, 25061, 25062, 25071, 25072, 25081, 25082, 25091, 25092, 25101, 25102, 25111, 25112, 25121, 25122, 26011, 26012, 26022, 26031]
importar_dne(arquivos_gud, colunas_gud, 'dne_delta_grande_usuario', dtype_gud, versoes_gud)