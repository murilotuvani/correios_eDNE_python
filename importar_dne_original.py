from sqlalchemy import INTEGER, create_engine, Integer, String, sql
import pandas as pd
from urllib.parse import quote
import unicodedata
import re
import sqlalchemy

def salvar(df, table_name, if_exists='replace', dtype=None):
    sqlEngine = create_engine('mysql+pymysql://root:root@10.50.1.128:3306/ceps', pool_recycle=3600)
    dbConnection    = sqlEngine.connect()
    transaction = dbConnection.begin()
    try:
        frame= df.to_sql(table_name, dbConnection, if_exists=if_exists, dtype=dtype, index=False)
        transaction.commit()
    except ValueError as vx:
        print(vx)
    except Exception as ex:
        print(ex)
    else:
        print("Table %s created successfully."%table_name);   
    finally:
        dbConnection.close()


#FAIXA CEP DE UF
arquivo1 = 'C:\\projetos\\Delimitado\\LOG_FAIXA_UF.TXT'
dtype1 = {
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'ufe_cep_ini': sqlalchemy.types.CHAR(8),
        'ufe_cep_fim': sqlalchemy.types.CHAR(8)
     }
dffu = pd.read_csv(arquivo1,
                            sep= '@',
                            names= ['ufe_sg', 'ufe_cep_ini', 'ufe_cep_fim'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dffu['arquivo'] = 25012
salvar(dffu, 'dne_faixa_uf', dtype= dtype1)


#LOCALIDADE
arquivo2 = 'C:\\projetos\\Delimitado\\LOG_LOCALIDADE.TXT'
dtype2 = {
        'loc_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_no': sqlalchemy.types.VARCHAR(72),
        'cep': sqlalchemy.types.CHAR(8),
        'loc_in_sit': sqlalchemy.types.CHAR(1),
        'loc_in_tipo_loc': sqlalchemy.types.CHAR(1),
        'loc_nu_sub': sqlalchemy.INTEGER(),
        'loc_no_abrev': sqlalchemy.types.VARCHAR(36),
        'mun_nu': sqlalchemy.types.CHAR(7)
     }
dffl = pd.read_csv(arquivo2,
                            sep= '@',
                            names= ['loc_nu', 'ufe_sg', 'loc_no', 'cep', 'loc_in_sit', 'loc_in_tipo_loc', 'loc_nu_sub', 'loc_no_abrev', 'mun_nu'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dffl['arquivo'] = 25012
salvar(dffl, 'dne_localidade', dtype= dtype2)


#OUTRAS DENOMINAÇÕES DA LOCALIDADE
arquivo3 = 'C:\\projetos\\Delimitado\\LOG_VAR_LOC.TXT'
dtype3 = {
        'loc_nu': sqlalchemy.INTEGER(),
        'val_nu': sqlalchemy.INTEGER(),
        'val_tx': sqlalchemy.types.VARCHAR(72)
     }
dfvl = pd.read_csv(arquivo3,
                            sep= '@',
                            names= ['loc_nu', 'val_nu', 'val_tx'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfvl['arquivo'] = 25012
salvar(dfvl, 'dne_var_loc', dtype= dtype3)


#FAIXA DE CEP DAS LOCALIDADES CODIFICADAS
arquivo4 = 'C:\\projetos\\Delimitado\\LOG_FAIXA_LOCALIDADE.TXT'
dtype4 = {
        'loc_nu': sqlalchemy.INTEGER(),
        'loc_cep_ini': sqlalchemy.types.CHAR(8),
        'loc_cep_fim': sqlalchemy.types.CHAR(8),
        'loc_tipo_faixa': sqlalchemy.types.CHAR(1)
     }
dffl2 = pd.read_csv(arquivo4,
                            sep= '@',
                            names= ['loc_nu', 'loc_cep_ini', 'loc_cep_fim', 'loc_tipo_faixa'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dffl2['arquivo'] = 25012
salvar(dffl2, 'dne_faixa_localidade', dtype= dtype4)


#BAIRRO
arquivo5 = 'C:\\projetos\\Delimitado\\LOG_BAIRRO.TXT'
dtype5 = {
        'bai_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_no': sqlalchemy.types.VARCHAR(72),
        'bai_no_abrev': sqlalchemy.types.VARCHAR(36)
     }
dfb = pd.read_csv(arquivo5,
                            sep= '@',
                            names= ['bai_nu', 'ufe_sg', 'loc_nu', 'bai_no', 'bai_no_abrev'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfb['arquivo'] = 25012
salvar(dfb, 'dne_bairro', dtype= dtype5)


#OUTRAS DENOMINAÇÕES DE BAIRRO
arquivo6 = 'C:\\projetos\\Delimitado\\LOG_VAR_BAI.TXT'
dtype6 = {
        'bai_nu': sqlalchemy.INTEGER(),
        'vdb_nu': sqlalchemy.INTEGER(),
        'vdb_tx': sqlalchemy.types.VARCHAR(72)
     }
dfvb = pd.read_csv(arquivo6,
                            sep= '@',
                            names= ['bai_nu', 'vdb_nu', 'vdb_tx'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfvb['arquivo'] = 25012
salvar(dfvb, 'dne_var_bairro', dtype= dtype6)


#FAIXA DE CEP DE BAIRRO
arquivo7 = 'C:\\projetos\\Delimitado\\LOG_FAIXA_BAIRRO.TXT'
dtype7 = {
        'bai_nu': sqlalchemy.INTEGER(),
        'fcb_cep_ini': sqlalchemy.types.CHAR(8),
        'fcb_cep_fim': sqlalchemy.types.CHAR(8),
     }
dffb = pd.read_csv(arquivo7,
                            sep= '@',
                            names= ['bai_nu', 'fcb_cep_ini', 'fcb_cep_fim'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dffb['arquivo'] = 25012
salvar(dffb, 'dne_faixa_bairro', dtype= dtype7)


#CAIXA POSTAL COMUNITÁRIA
arquivo8 = 'C:\\projetos\\Delimitado\\LOG_CPC.TXT'
dtype8 = {
        'cpc_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'cpc_no': sqlalchemy.types.VARCHAR(72),
        'cpc_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8)
     }
dfcpc = pd.read_csv(arquivo8,
                            sep= '@',
                            names= ['cpc_nu', 'ufe_sg', 'loc_nu', 'cpc_no', 'cpc_endereco', 'cep'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfcpc['arquivo'] = 25012
salvar(dfcpc, 'dne_cpc', dtype= dtype8)


#FAIXA CAIXA POSTAL COMUNITÁRIA
arquivo9 = 'C:\\projetos\\Delimitado\\LOG_FAIXA_CPC.TXT'
dtype9 = {
        'cpc_nu': sqlalchemy.INTEGER(),
        'cpc_inicial': sqlalchemy.types.CHAR(6),
        'cpc_final': sqlalchemy.types.CHAR(6)
     }
dffcpc = pd.read_csv(arquivo9,
                            sep= '@',
                            names= ['cpc_nu', 'cpc_inicial', 'cpc_final'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dffcpc['arquivo'] = 25012
salvar(dffcpc, 'dne_faixa_cpc', dtype= dtype9)


#FAIXA NUMÉRICA DO SECCIONAMENTO
arquivo10 = 'C:\\projetos\\Delimitado\\LOG_NUM_SEC.TXT'
dtype10 = {
        'log_nu': sqlalchemy.INTEGER(),
        'sec_nu_ini': sqlalchemy.types.VARCHAR(10),
        'sec_nu_fim': sqlalchemy.types.VARCHAR(10),
        'sec_in_lado': sqlalchemy.types.CHAR(1)
     }
dfns = pd.read_csv(arquivo10,
                            sep= '@',
                            names= ['log_nu', 'sec_nu_ini', 'sec_nu_fim', 'sec_in_lado'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfns['arquivo'] = 25012
salvar(dfns, 'dne_num_sec', dtype= dtype10)


#GRANDE USUÁRIO
arquivo11 = 'C:\\projetos\\Delimitado\\LOG_GRANDE_USUARIO.TXT'
dtype11 = {
        'gru_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_nu': sqlalchemy.INTEGER(),
        'log_nu': sqlalchemy.INTEGER(),
        'gru_no': sqlalchemy.types.VARCHAR(72),
        'gru_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'gru_no_abrev': sqlalchemy.types.VARCHAR(36)
     }
dfgu = pd.read_csv(arquivo11,
                            sep= '@',
                            names= ['gru_nu', 'ufe_sg', 'loc_nu', 'bai_nu', 'log_nu', 'gru_no', 'gru_endereco', 'cep', 'gru_no_abrev'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfgu['arquivo'] = 25012
salvar(dfgu, 'dne_grande_usuario', dtype= dtype11)


#UNIDADE OPERACIONAL DOS CORREIOS
arquivo12 = 'C:\\projetos\\Delimitado\\LOG_UNID_OPER.TXT'
dtype12 = {
        'uop_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_nu': sqlalchemy.INTEGER(),
        'log_nu': sqlalchemy.INTEGER(),
        'uop_no': sqlalchemy.types.VARCHAR(100),
        'uop_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'uop_in_cp': sqlalchemy.types.CHAR(1),
        'uop_no_abrev': sqlalchemy.types.VARCHAR(36)
     }
dfuo = pd.read_csv(arquivo12,
                            sep= '@',
                            names= ['uop_nu', 'ufe_sg', 'loc_nu', 'bai_nu', 'log_nu', 'uop_no', 'uop_endereco', 'cep', 'uop_in_cp', 'uop_no_abrev'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfuo['arquivo'] = 25012
salvar(dfuo, 'dne_unidade_operacional', dtype= dtype12)


#FAIXA DE CAIXA POSTAL
arquivo13 = 'C:\\projetos\\Delimitado\\LOG_FAIXA_UOP.TXT'
dtype13 = {
        'uop_nu': sqlalchemy.INTEGER(),
        'fnc_inicial': sqlalchemy.INTEGER(),
        'fnc_final': sqlalchemy.INTEGER()
     }
dffu2 = pd.read_csv(arquivo13,
                            sep= '@',
                            names= ['uop_nu', 'fnc_inicial', 'fnc_final'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dffu2['arquivo'] = 25012
salvar(dffu2, 'dne_faixa_uop', dtype= dtype13)


#RELAÇÃO DOS NOMES DOS PAÍSES
arquivo14 = 'C:\\projetos\\Delimitado\\ECT_PAIS.TXT'
dtype14 = {
        'pai_sg': sqlalchemy.types.CHAR(2),
        'pai_sg_alternativa': sqlalchemy.types.CHAR(3),
        'pai_no_portugues': sqlalchemy.types.CHAR(72),
        'pai_no_ingles': sqlalchemy.types.CHAR(72),
        'pai_no_frances': sqlalchemy.types.CHAR(72),
        'pai_abreviatura': sqlalchemy.types.CHAR(36)
     }
dfectp = pd.read_csv(arquivo14,
                            sep= '@',
                            names= ['pai_sg', 'pai_sg_alternativa', 'pai_no_portugues', 'pai_no_ingles', 'pai_no_frances', 'pai_abreviatura'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfectp['arquivo'] = 25012
salvar(dfectp, 'dne_ectpaises', dtype= dtype14)


#OUTRAS DENOMINAÇÕES DO LOGRADOURO
arquivo15 = 'C:\\projetos\\Delimitado\\LOG_VAR_LOG.TXT'
dtype15 = {
        'log_nu': sqlalchemy.INTEGER(),
        'vlo_nu': sqlalchemy.INTEGER(),
        'tlo_tx': sqlalchemy.types.VARCHAR(36),
        'vlo_tx': sqlalchemy.types.VARCHAR(150)
     }
dfvl2 = pd.read_csv(arquivo15,
                            sep= '@',
                            names= ['log_nu', 'vlo_nu', 'tlo_tx', 'vlo_tx'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfvl2['arquivo'] = 25012
salvar(dfvl2, 'dne_var_log', dtype= dtype15)


#LOGRADOUROS-SP
arquivo161 = 'C:\\projetos\\Delimitado\\LOG_LOGRADOURO_SP.TXT'
dtype161 = {
        'log_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_nu_ini': sqlalchemy.INTEGER(),
        'bai_nu_fim': sqlalchemy.INTEGER(),
        'log_no': sqlalchemy.types.VARCHAR(100),
        'log_complemento': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'tlo_tx': sqlalchemy.types.VARCHAR(36),
        'log_sta_tlo': sqlalchemy.types.CHAR(1),
        'log_no_abrev': sqlalchemy.types.VARCHAR(36)
     }
dflogsp = pd.read_csv(arquivo161,
                            sep= '@',
                            names= ['log_nu', 'ufe_sg', 'loc_nu', 'bai_nu_ini', 'bai_nu_fim', 'log_no', 'log_complemento', 'cep', 'tlo_tx', 'log_sta_tlo', 'log_no_abrev'],
                            header=None, 
                            encoding='ISO-8859-1'
                )
dflogsp['arquivo'] = 25012
salvar(dflogsp, 'dne_logradouros_sp', dtype= dtype161)


#LOGRADOUROS
arquivos = [ "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_AC.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_AL.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_AM.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_AP.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_BA.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_CE.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_DF.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_ES.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_GO.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_MA.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_MG.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_MS.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_TO.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_MT.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_PA.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_PB.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_PE.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_PI.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_PR.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_RJ.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_RN.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_RO.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_RR.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_RS.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_SC.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_SE.TXT",
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_SP.TXT"
 ]
dtype16 = {
        'log_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_nu_ini': sqlalchemy.INTEGER(),
        'bai_nu_fim': sqlalchemy.INTEGER(),
        'log_no': sqlalchemy.types.VARCHAR(100),
        'log_complemento': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'tlo_tx': sqlalchemy.types.VARCHAR(36),
        'log_sta_tlo': sqlalchemy.types.CHAR(1),
        'log_no_abrev': sqlalchemy.types.VARCHAR(36)
     }
dflog= pd.concat([pd.read_csv(arquivos, 
                              sep='@', 
                              names=['log_nu', 'ufe_sg', 'loc_nu', 'bai_nu_ini', 'bai_nu_fim', 'log_no', 'log_complemento', 'cep', 'tlo_tx', 'log_sta_tlo', 'log_no_abrev'], 
                              header=None, 
                              encoding='ISO-8859-1') 
                for arquivos in arquivos])
dflog['arquivo'] = 25012
salvar(dflog, 'dne_logradouros', dtype= dtype16)