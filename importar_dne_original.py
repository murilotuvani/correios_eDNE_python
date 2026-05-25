from sqlalchemy import INTEGER, create_engine, Integer, String, sql
import pandas as pd
from urllib.parse import quote
import unicodedata
import re
import sqlalchemy

#função importada de 'importar_dne.py'
def salvar(df, table_name, if_exists='replace', dtype=None):
    sqlEngine = create_engine('mysql+pymysql://root:root@10.50.1.252:3306/ceps', pool_recycle=3600)
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

#função para importar pro MySQL (mais prático)
#coloca o caminho do(s) arquivo(s)
arquivos_x= []
#nome das colunas
colunas_x= []
#o tipo das colunas (VARCHAR, INTEGER(), CHAR etc)
dtype_x= {} #precisa ter a coluna que mostra qual a atualização do arquivo (vai ser 'atualizacao': sqlalchemy.INTEGER()
versoes_x= [] #número da atualização do arquivo
def importar_dne (arquivos, colunas, tabela, dtype, versoes):
    #lista que vai armazenar os dfs
    dfs= []
    #percorrer cada arquivo na lista
    for arquivo, versao in zip (arquivos, versoes):
        df= pd.read_csv(arquivo,
                sep= '@',
                names= colunas,
                header= None,
                encoding= 'ISO-8859-1'
                )
        #cria coluna chamada 'atualizacao'; todas as linhas vão ter o número da atualização
        df['atualizacao']= versao
        #adiciona df na lista
        dfs.append(df)
    #pra ver quantos dfs existem
    #se tiver 1 (if), se tiver vários (else)
    if len(dfs)== 1:
        df_final= dfs[0]
    else:
        #junta todos os dfs caso tenha mais de um
        df_final= pd.concat(dfs,
                    #reorganiza os indices
                    ignore_index= True
                )
    df_final = df_final.drop_duplicates()
    #chama função salvar()
    salvar(df_final, tabela, dtype= dtype)


#FAIXA CEP DE UF
arquivos_faixauf = ["C:\\projetos\\Delimitado\\LOG_FAIXA_UF.TXT"]
colunas_faixauf = ['ufe_sg', 'ufe_cep_ini', 'ufe_cep_fim']
dtype_faixauf= {
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'ufe_cep_ini': sqlalchemy.types.CHAR(8),
        'ufe_cep_fim': sqlalchemy.types.CHAR(8),
        'atualizacao': sqlalchemy.INTEGER()
        }
versoes_faixauf= [25012]
importar_dne(arquivos_faixauf, colunas_faixauf, 'dne_faixa_uf', dtype_faixauf, versoes_faixauf)

#LOCALIDADE
arquivos_localidade = ["C:\\projetos\\Delimitado\\LOG_LOCALIDADE.TXT"]
colunas_localidade = ['loc_nu', 'ufe_sg', 'loc_no', 'cep', 'loc_in_sit', 'loc_in_tipo_loc', 'loc_nu_sub', 'loc_no_abrev', 'mun_nu']
dtype_localidade= {
        'loc_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_no': sqlalchemy.types.VARCHAR(72),
        'cep': sqlalchemy.types.CHAR(8),
        'loc_in_sit': sqlalchemy.types.CHAR(1),
        'loc_in_tipo_loc': sqlalchemy.types.CHAR(1),
        'loc_nu_sub': sqlalchemy.INTEGER(),
        'loc_no_abrev': sqlalchemy.types.VARCHAR(36),
        'mun_nu': sqlalchemy.types.CHAR(7),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_localidade= [25012]
importar_dne(arquivos_localidade, colunas_localidade, 'dne_localidade', dtype_localidade, versoes_localidade)

#OUTRAS DENOMINAÇÕES DA LOCALIDADE
arquivos_varloc = ["C:\\projetos\\Delimitado\\LOG_VAR_LOC.TXT"]
colunas_varloc = ['loc_nu', 'val_nu', 'val_tx']
dtype_varloc= {
        'loc_nu': sqlalchemy.INTEGER(),
        'val_nu': sqlalchemy.INTEGER(),
        'val_tx': sqlalchemy.types.VARCHAR(72),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_varloc= [25012]
importar_dne(arquivos_varloc, colunas_varloc, 'dne_var_loc', dtype_varloc, versoes_varloc)

#FAIXA DE CEP DAS LOCALIDADES CODIFICADAS
arquivos_faixalocalidade = ["C:\\projetos\\Delimitado\\LOG_FAIXA_LOCALIDADE.TXT"]
colunas_faixalocalidade = ['loc_nu', 'loc_cep_ini', 'loc_cep_fim', 'loc_tipo_faixa']
dtype_faixalocalidade= {
        'loc_nu': sqlalchemy.INTEGER(),
        'loc_cep_ini': sqlalchemy.types.CHAR(8),
        'loc_cep_fim': sqlalchemy.types.CHAR(8),
        'loc_tipo_faixa': sqlalchemy.types.CHAR(1),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_faixalocalidade= [25012]
importar_dne(arquivos_faixalocalidade, colunas_faixalocalidade, 'dne_faixa_localidade', dtype_faixalocalidade, versoes_faixalocalidade)

#BAIRRO
arquivos_bairro = ["C:\\projetos\\Delimitado\\LOG_BAIRRO.TXT"]
colunas_bairro = ['bai_nu', 'ufe_sg', 'loc_nu', 'bai_no', 'bai_no_abrev']
dtype_bairro= {
        'bai_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_no': sqlalchemy.types.VARCHAR(72),
        'bai_no_abrev': sqlalchemy.types.VARCHAR(36),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_bairro= [25012]
importar_dne(arquivos_bairro, colunas_bairro, 'dne_bairro', dtype_bairro, versoes_bairro)

#OUTRAS DENOMINAÇÕES DE BAIRRO
arquivos_varbairro = ["C:\\projetos\\Delimitado\\LOG_VAR_BAI.TXT"]
colunas_varbairro = ['bai_nu', 'vdb_nu', 'vdb_tx']
dtype_varbairro= {
        'bai_nu': sqlalchemy.INTEGER(),
        'vdb_nu': sqlalchemy.INTEGER(),
        'vdb_tx': sqlalchemy.types.VARCHAR(72),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_varbairro= [25012]
importar_dne(arquivos_varbairro, colunas_varbairro, 'dne_var_bairro', dtype_varbairro, versoes_varbairro)

#FAIXA CEP DE BAIRRO
arquivos_faixabairro = ["C:\\projetos\\Delimitado\\LOG_FAIXA_BAIRRO.TXT"]
colunas_faixabairro = ['bai_nu', 'fcb_cep_ini', 'fcb_cep_fim']
dtype_faixabairro= {
        'bai_nu': sqlalchemy.INTEGER(),
        'fcb_cep_ini': sqlalchemy.types.CHAR(8),
        'fcb_cep_fim': sqlalchemy.types.CHAR(8),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_faixabairro= [25012]
importar_dne(arquivos_faixabairro, colunas_faixabairro, 'dne_faixa_bairro', dtype_faixabairro, versoes_faixabairro)

#CAIXA POSTAL COMUNITÁRIA
arquivos_cpc = ["C:\\projetos\\Delimitado\\LOG_CPC.TXT"]
colunas_cpc = ['cpc_nu', 'ufe_sg', 'loc_nu', 'cpc_no', 'cpc_endereco', 'cep']
dtype_cpc= {
        'cpc_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'cpc_no': sqlalchemy.types.VARCHAR(72),
        'cpc_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_cpc= [25012]
importar_dne(arquivos_cpc, colunas_cpc, 'dne_cpc', dtype_cpc, versoes_cpc)

#FAIXA DE CAIXA POSTAL COMUNITÁRIA
arquivos_faixacpc = ["C:\\projetos\\Delimitado\\LOG_FAIXA_CPC.TXT"]
colunas_faixacpc = ['cpc_nu', 'cpc_inicial', 'cpc_final']
dtype_faixacpc= {
        'cpc_nu': sqlalchemy.INTEGER(),
        'cpc_inicial': sqlalchemy.types.CHAR(6),
        'cpc_final': sqlalchemy.types.CHAR(6),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_faixacpc= [25012]
importar_dne(arquivos_faixacpc, colunas_faixacpc, 'dne_faixa_cpc', dtype_faixacpc, versoes_faixacpc)

#LOGRADOUROS
arquivos_logradouros = ["C:\\projetos\\Delimitado\\LOG_LOGRADOURO_AC.TXT",
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
 "C:\\projetos\\Delimitado\\LOG_LOGRADOURO_SP.TXT"]
colunas_logradouros = ['log_nu', 'ufe_sg', 'loc_nu', 'bai_nu_ini', 'bai_nu_fim', 'log_no', 'log_complemento', 'cep', 'tlo_tx', 'log_sta_tlo', 'log_no_abrev']
dtype_logradouros= {
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
        'log_no_abrev': sqlalchemy.types.VARCHAR(36),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_logradouros= [25012]
importar_dne(arquivos_logradouros, colunas_logradouros, 'dne_logradouro', dtype_logradouros, versoes_logradouros)

#OUTRAS DENOMINAÇÕES DO LOGRADOURO
arquivos_varlog = ["C:\\projetos\\Delimitado\\LOG_VAR_LOG.TXT"]
colunas_varlog = ['log_nu', 'vlo_nu', 'tlo_tx', 'vlo_tx']
dtype_varlog= {
        'log_nu': sqlalchemy.INTEGER(),
        'vlo_nu': sqlalchemy.INTEGER(),
        'tlo_tx': sqlalchemy.types.VARCHAR(36),
        'vlo_tx': sqlalchemy.types.VARCHAR(150),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_varlog= [25012]
importar_dne(arquivos_varlog, colunas_varlog, 'dne_var_log', dtype_varlog, versoes_varlog)

#FAIXA NUMÉRICA DO SECCIONAMENTO
arquivos_numsec = ["C:\\projetos\\Delimitado\\LOG_NUM_SEC.TXT"]
colunas_numsec = ['log_nu', 'sec_nu_ini', 'sec_nu_fim', 'sec_in_lado']
dtype_numsec= {
        'log_nu': sqlalchemy.INTEGER(),
        'sec_nu_ini': sqlalchemy.types.VARCHAR(10),
        'sec_nu_fim': sqlalchemy.types.VARCHAR(10),
        'sec_in_lado': sqlalchemy.types.CHAR(1),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_numsec= [25012]
importar_dne(arquivos_numsec, colunas_numsec, 'dne_num_sec', dtype_numsec, versoes_numsec)

#GRANDE USUÁRIO
arquivos_gu = ["C:\\projetos\\Delimitado\\LOG_GRANDE_USUARIO.TXT"]
colunas_gu = ['gru_nu', 'ufe_sg', 'loc_nu', 'bai_nu', 'log_nu', 'gru_no', 'gru_endereco', 'cep', 'gru_no_abrev']
dtype_gu= {
        'gru_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_nu': sqlalchemy.INTEGER(),
        'log_nu': sqlalchemy.INTEGER(),
        'gru_no': sqlalchemy.types.VARCHAR(72),
        'gru_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'gru_no_abrev': sqlalchemy.types.VARCHAR(36),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_gu= [25012]
importar_dne(arquivos_gu, colunas_gu, 'dne_grande_usuario', dtype_gu, versoes_gu)

#UNIDADE OPERACIONAL DOS CORREIOS
arquivos_uop = ["C:\\projetos\\Delimitado\\LOG_UNID_OPER.TXT"]
colunas_uop = ['uop_nu', 'ufe_sg', 'loc_nu', 'bai_nu', 'log_nu', 'uop_no', 'uop_endereco', 'cep', 'uop_in_cp', 'uop_no_abrev']
dtype_uop= {
        'uop_nu': sqlalchemy.INTEGER(),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.INTEGER(),
        'bai_nu': sqlalchemy.INTEGER(),
        'log_nu': sqlalchemy.INTEGER(),
        'uop_no': sqlalchemy.types.VARCHAR(100),
        'uop_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'uop_in_cp': sqlalchemy.types.CHAR(1),
        'uop_no_abrev': sqlalchemy.types.VARCHAR(36),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_uop= [25012]
importar_dne(arquivos_uop, colunas_uop, 'dne_unidade_operacional', dtype_uop, versoes_uop)

#FAIXA DE CAIXA POSTAL
arquivos_faixauop = ["C:\\projetos\\Delimitado\\LOG_FAIXA_UOP.TXT"]
colunas_faixauop = ['uop_nu', 'fnc_inicial', 'fnc_final']
dtype_faixauop= {
        'uop_nu': sqlalchemy.INTEGER(),
        'fnc_inicial': sqlalchemy.INTEGER(),
        'fnc_final': sqlalchemy.INTEGER(),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_faixauop= [25012]
importar_dne(arquivos_faixauop, colunas_faixauop, 'dne_faixa_uop', dtype_faixauop, versoes_faixauop)

#RELAÇÃO DOS NOMES DOS PAÍSES
arquivos_ectpaises = ["C:\\projetos\\Delimitado\\ECT_PAIS.TXT"]
colunas_ectpaises = ['pai_sg', 'pai_sg_alternativa', 'pai_no_portugues', 'pai_no_ingles', 'pai_no_frances', 'pai_abreviatura']
dtype_ectpaises= {
        'pai_sg': sqlalchemy.types.CHAR(2),
        'pai_sg_alternativa': sqlalchemy.types.CHAR(3),
        'pai_no_portugues': sqlalchemy.types.CHAR(72),
        'pai_no_ingles': sqlalchemy.types.CHAR(72),
        'pai_no_frances': sqlalchemy.types.CHAR(72),
        'pai_abreviatura': sqlalchemy.types.CHAR(36),
        'atualizacao': sqlalchemy.INTEGER()
}
versoes_ectpaises= [25012]
importar_dne(arquivos_ectpaises, colunas_ectpaises, 'dne_ect_pais', dtype_ectpaises, versoes_ectpaises)