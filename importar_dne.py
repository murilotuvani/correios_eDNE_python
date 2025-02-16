import pandas as pd
import pymysql
from sqlalchemy import create_engine
import os
import sqlalchemy

def salvar(df, table_name, if_exists='replace', dtype=None):
    sqlEngine = create_engine('mysql+pymysql://root:root@127.0.0.1:3306/cep', pool_recycle=3600)
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
UFE_SG	sigla da UF	CHAR(2)
SEPARADOR	@	
UFE_CEP_INI	CEP inicial da UF	CHAR(8)
SEPARADOR	@	
UFE_CEP_FIM	CEP final da UF	CHAR(8)
'''
def importa_uf_faixa(file):
    colunas = ['ufe_sg','ufe_cep_ini','ufe_cep_fim']

    dtype = {
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'ufe_cep_ini': sqlalchemy.types.CHAR(8),
        'ufe_cep_fim': sqlalchemy.types.CHAR(8)
    }

    df = pd.read_csv(file, header=None, index_col='ufe_sg', names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_uf_faixa', dtype=dtype)



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

'''
2.2.	LOG_LOCALIDADE.TXT 
CAMPO	DESCRIÇÃO DO CAMPO	TIPO
LOC_NU	chave da localidade	NUMBER(8)
SEPARADOR	@	
UFE_SG	sigla da UF	CHAR(2)
SEPARADOR	@	
LOC_NO	nome da localidade	VARCHAR(72)
SEPARADOR	@	
CEP	CEP da localidade (para  localidade  não codificada, ou seja loc_in_sit = 0) (opcional)	CHAR(8)
SEPARADOR	@	
LOC_IN_SIT	situação da localidade:
0 = Localidade  não codificada em nível de Logradouro,
1 = Localidade codificada em nível de Logradouro e
2 = Distrito ou Povoado inserido na codificação em nível de Logradouro.
3 = Localidade em fase de codificação  em nível de Logradouro.	CHAR(1)
SEPARADOR	@	

LOC_IN_TIPO_LOC	tipo de localidade:
D – Distrito,
M – Município,
P – Povoado.	
CHAR(1)

SEPARADOR	@	
LOC_NU_SUB	chave da localidade de subordinação (opcional)	NUMBER(8)
SEPARADOR	@	
LOC_NO_ABREV	abreviatura do nome da localidade (opcional)	VARCHAR(36)
SEPARADOR	@	
MUN_NU	Código do município IBGE (opcional)	CHAR(7)


'''

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

'''
2.3.	LOG_VAR_LOC.TXT – Outras denominações da Localidade (denominação popular, denominação anterior)
CAMPO	DESCRIÇÃO DO CAMPO	TIPO
LOC_NU	chave da localidade	NUMBER(8)
SEPARADOR	@	
VAL_NU	ordem da denominação	NUMBER(8)
SEPARADOR	@	
VAL_TX	Denominação	VARCHAR2(72)
'''

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

'''
CAMPO	DESCRIÇÃO DO CAMPO	TIPO
LOC_NU	chave da localidade	NUMBER(8)
SEPARADOR	@	
LOC_CEP_INI	CEP inicial da localidade	CHAR(8)
SEPARADOR	@	
LOC_CEP_FIM	CEP final da localidade	CHAR(8)
SEPARADOR	@	
LOC_TIPO_FAIXA	tipo de Faixa de CEP:
T –Total do Município 
C – Exclusiva da  Sede Urbana	CHAR(1)
'''
def importa_localidade_faixa(file):
    colunas = ['loc_nu','loc_cep_ini', 'loc_cep_fim', 'loc_tipo_faixa']

    dtype = {
        'loc_nu': sqlalchemy.types.NUMERIC(8),
        'loc_nu_ini': sqlalchemy.types.NUMERIC(8),
        'loc_nu_fim': sqlalchemy.types.NUMERIC(8),
        'loc_tipo_faixa': sqlalchemy.types.CHAR(1)
    }

    df = pd.read_csv(file, header=None, index_col=['loc_nu','loc_nu_ini'], names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_localidade_faixa', dtype=dtype)

'''
CAMPO	DESCRIÇÃO DO CAMPO	TIPO
BAI_NU	chave do bairro	NUMBER(8)
SEPARADOR	@	
UFE_SG	sigla da UF	CHAR(2)
SEPARADOR	@	
LOC_NU	chave da localidade	NUMBER(8)
SEPARADOR	@	
BAI_NO	nome do bairro	VARCHAR2(72)
SEPARADOR	@	
BAI_NO_ABREV	abreviatura do nome do bairro (opcional)	VARCHAR2(36)
'''
def importa_bairro(file):
    colunas = ['bai_nu','ufe_sg', 'loc_nu', 'bai_no', 'bai_no_abrev']

    dtype = {
        'bai_nu': sqlalchemy.types.NUMERIC(8),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.types.NUMERIC(8),
        'bai_no': sqlalchemy.types.VARCHAR(72),
        'bai_no_abrev': sqlalchemy.types.VARCHAR(36)
    }

    df = pd.read_csv(file, header=None, index_col='bai_nu', names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_bairro', dtype=dtype)

'''
CAMPO	DESCRIÇÃO DO CAMPO	TIPO
BAI_NU	chave do bairro	NUMBER(8)
SEPARADOR	@	
VDB_NU	ordem da denominação	NUMBER(8)
SEPARADOR	@	
VDB_TX	Denominação	VARCHAR2(72)
'''

def importa_bairro_variacao(file):
    colunas = ['bai_nu','vdb_nu', 'vdb_tx']

    dtype = {
        'bai_nu': sqlalchemy.types.NUMERIC(8),
        'vdb_nu': sqlalchemy.types.NUMERIC(8),
        'vdb_tx': sqlalchemy.types.VARCHAR(72)
    }

    df = pd.read_csv(file, header=None, index_col=['bai_nu','vdb_nu'], names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_bairro_variacao', dtype=dtype)

'''
2.7.	LOG_FAIXA_BAIRRO.TXT – Faixa de CEP de Bairro
CAMPO	DESCRIÇÃO DO CAMPO	TIPO
BAI_NU	chave do bairro	NUMBER(8)
SEPARADOR	@	
FCB_CEP_INI	CEP inicial do bairro	CHAR(8)
SEPARADOR	@	
FCB_CEP_FIM	CEP final do bairro	CHAR(8)
'''
def importa_bairro_faixa(file):
    colunas = ['bai_nu','fcb_cep_ini', 'fcb_cep_fim']

    dtype = {
        'bai_nu': sqlalchemy.types.NUMERIC(8),
        'fcb_cep_ini': sqlalchemy.types.CHAR(8),
        'fcb_cep_fim': sqlalchemy.types.CHAR(8)
    }

    df = pd.read_csv(file, header=None, index_col=['bai_nu','fcb_cep_ini'], names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_bairro_faixa', dtype=dtype)

'''
2.8.	LOG_CPC.TXT –  Caixa Postal Comunitária(CPC) - são áreas rurais e/ou urbanas periféricas não atendidas pela distribuição domiciliária.

CAMPO	DESCRIÇÃO DO CAMPO	TIPO
CPC_NU	chave da caixa postal comunitária	NUMBER(8)
SEPARADOR	@	
UFE_SG	sigla da UF	CHAR(2)
SEPARADOR	@	
LOC_NU	chave da localidade	NUMBER(8)
SEPARADOR	@	
CPC_NO	nome da CPC	VARCHAR2(72)
SEPARADOR	@	
CPC_ENDERECO	endereço da CPC	VARCHAR2(100)
SEPARADOR	@	
CEP	CEP da CPC	CHAR(8)
'''
def importa_caixa_postal_comunitaria(file):
    colunas = ['cpc_nu','ufe_sg', 'loc_nu', 'cpc_no', 'cpc_endereco', 'cep']

    dtype = {
        'cpc_nu': sqlalchemy.types.NUMERIC(8),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.types.NUMERIC(8),
        'cpc_no': sqlalchemy.types.VARCHAR(72),
        'cpc_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8)
    }

    df = pd.read_csv(file, header=None, index_col='cpc_nu', names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_caixa_postal_comunitaria', dtype=dtype)

'''
2.9.	LOG_FAIXA_CPC.TXT – Faixa de Caixa Postal Comunitária
CAMPO	DESCRIÇÃO DO CAMPO	TIPO
CPC_NU	chave da caixa postal comunitária	NUMBER(8)
SEPARADOR	@	
CPC_INICIAL	número inicial da caixa postal comunitária	VARCHAR2(6)
SEPARADOR	@	
CPC_FINAL	número final da caixa postal comunitária	VARCHAR2(6)
'''
def importa_caixa_postal_comunitaria_faixa(file):
    colunas = ['cpc_nu','cpc_inicial', 'cpc_final']

    dtype = {
        'cpc_nu': sqlalchemy.types.NUMERIC(8),
        'cpc_inicial': sqlalchemy.types.VARCHAR(6),
        'cpc_final': sqlalchemy.types.VARCHAR(6)
    }

    df = pd.read_csv(file, header=None, index_col=['cpc_nu','cpc_inicial'], names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_caixa_postal_comunitaria_faixa', dtype=dtype)

'''
2.12.	LOG_NUM_SEC.TXT – Faixa numérica do seccionamento
CAMPO	DESCRIÇÃO DO CAMPO	TIPO
LOG_NU	chave do logradouro	NUMBER(8)
SEPARADOR	@	
SEC_NU_INI	número inicial do seccionamento	VARCHAR2(10)
SEPARADOR	@	
SEC_NU_FIM	número final do seccionamento	VARCHAR2(10)
SEPARADOR	@	
SEC_IN_LADO	Indica a paridade/lado do seccionamento
A – ambos,
P – par,
I – ímpar,
D – direito e
E – esquerdo.	CHAR(1)
'''
def importa_seccionamento(file):
    colunas = ['log_nu','sec_nu_ini', 'sec_nu_fim', 'sec_in_lado']

    dtype = {
        'log_nu': sqlalchemy.types.NUMERIC(8),
        'sec_nu_ini': sqlalchemy.types.VARCHAR(10),
        'sec_nu_fim': sqlalchemy.types.VARCHAR(10),
        'sec_in_lado': sqlalchemy.types.CHAR(1)
    }

    df = pd.read_csv(file, header=None, index_col=['log_nu','sec_nu_ini'], names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_seccionamento', dtype=dtype)

'''
2.13.	LOG_GRANDE_USUARIO.TXT – Grande Usuário 
		São clientes com grande volume postal (empresas, universidades, bancos,  órgãos públicos, etc), O campo LOG_NU está sem conteúdo para as localidades não codificadas(LOC_IN_SIT=0), devendo ser utilizado o campo GRU_ENDEREÇO para  endereçamento.

CAMPO	DESCRIÇÃO DO CAMPO	TIPO
GRU_NU	chave do grande usuário	NUMBER(8)
SEPARADOR	@	
UFE_SG	sigla da UF	CHAR(2)
SEPARADOR	@	
LOC_NU	chave da localidade	NUMBER(8)
SEPARADOR	@	
BAI_NU	chave do bairro	NUMBER(8)
SEPARADOR	@	
LOG_NU	chave do logradouro (opcional)	NUMBER(8)
SEPARADOR	@	
GRU_NO	nome do grande usuário	VARCHAR2(72)
SEPARADOR	@	
GRU_ENDERECO	endereço do grande usuário	VARCHAR2(100)
SEPARADOR	@	
CEP	CEP do grande usuário	CHAR(8)
SEPARADOR	@	
GRU_NO_ABREV	abreviatura do nome do grande usuário (opcional) 	VARCHAR2(36)
'''
def importa_grande_usuario(file):
    colunas = ['gru_nu','ufe_sg', 'loc_nu', 'bai_nu', 'log_nu', 'gru_no', 'gru_endereco', 'cep', 'gru_no_abrev']

    dtype = {
        'gru_nu': sqlalchemy.types.NUMERIC(8),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.types.NUMERIC(8),
        'bai_nu': sqlalchemy.types.NUMERIC(8),
        'log_nu': sqlalchemy.types.NUMERIC(8),
        'gru_no': sqlalchemy.types.VARCHAR(72),
        'gru_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'gru_no_abrev': sqlalchemy.types.VARCHAR(36)
    }

    df = pd.read_csv(file, header=None, index_col='gru_nu', names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_grande_usuario', dtype=dtype)

'''
2.14.	LOG_UNID_OPER.TXT – Unidade Operacional dos Correios. 
	São agências próprias ou terceirizadas, centros de distribuição, etc. O campo LOG_NU está sem conteúdo para as localidades não codificadas(LOC_IN_SIT=0), devendo ser utilizado o campo UOP_ENDEREÇO para endereçamento.

CAMPO	DESCRIÇÃO DO CAMPO	TIPO
UOP_NU	chave da UOP	NUMBER(8)
SEPARADOR	@	
UFE_SG	sigla da UF	CHAR(2)
SEPARADOR	@	
LOC_NU	chave da localidade	NUMBER(8)
SEPARADOR	@	
BAI_NU	chave do bairro	NUMBER(8)
SEPARADOR	@	
LOG_NU	chave do logradouro (opcional)	NUMBER(8)
SEPARADOR	@	
UOP_NO	nome da UOP	VARCHAR2(100)
SEPARADOR	@	
UOP_ENDERECO	endereço da UOP	VARCHAR2(100)
SEPARADOR	@	
CEP	CEP da UOP	CHAR(8)
SEPARADOR	@	
UOP_IN_CP	indicador de caixa postal (S ou N)	CHAR(1)
SEPARADOR	@	
UOP_NO_ABREV	abreviatura do nome da unid. operacional (opcional) 	VARCHAR2(36)
'''
def importa_unidade_operacional(file):
    colunas = ['uop_nu','ufe_sg', 'loc_nu', 'bai_nu', 'log_nu', 'uop_no', 'uop_endereco', 'cep', 'uop_in_cp', 'uop_no_abrev']

    dtype = {
        'uop_nu': sqlalchemy.types.NUMERIC(8),
        'ufe_sg': sqlalchemy.types.CHAR(2),
        'loc_nu': sqlalchemy.types.NUMERIC(8),
        'bai_nu': sqlalchemy.types.NUMERIC(8),
        'log_nu': sqlalchemy.types.NUMERIC(8),
        'uop_no': sqlalchemy.types.VARCHAR(100),
        'uop_endereco': sqlalchemy.types.VARCHAR(100),
        'cep': sqlalchemy.types.CHAR(8),
        'uop_in_cp': sqlalchemy.types.CHAR(1),
        'uop_no_abrev': sqlalchemy.types.VARCHAR(36)
    }

    df = pd.read_csv(file, header=None, index_col='uop_nu', names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_unidade_operacional', dtype=dtype)

'''
2.15.	LOG_FAIXA_UOP.TXT – Faixa de Caixa Postal – UOP
CAMPO	DESCRIÇÃO DO CAMPO	TIPO
UOP_NU	chave da UOP	NUMBER(8)
SEPARADOR	@	
FNC_INICIAL	número inicial da caixa postal 	NUMBER(8)
SEPARADOR	@	
FNC_FINAL	número final da caixa postal	NUMBER(8)
'''
def importa_unidade_operacional_faixa(file):
    colunas = ['uop_nu','fnc_inicial', 'fnc_final']

    dtype = {
        'uop_nu': sqlalchemy.types.NUMERIC(8),
        'fnc_inicial': sqlalchemy.types.NUMERIC(8),
        'fnc_final': sqlalchemy.types.NUMERIC(8)
    }

    df = pd.read_csv(file, header=None, index_col=['uop_nu','fnc_inicial'], names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_unidade_operacional_faixa', dtype=dtype)

'''
2.16.	ECT_PAIS.TXT – Relação dos Nomes dos Países, suas siglas e grafias em inglês e francês(*). 
CAMPO	DESCRIÇÃO DO CAMPO	TIPO
PAI_SG	Sigla do País	CHAR(2)
SEPARADOR	@	
PAI_SG_ALTERNATIVA	número inicial da caixa postal 	CHAR(3)
SEPARADOR	@	
PAI_NO_PORTUGUES	número final da caixa postal	CHAR(72)
SEPARADOR	@	
PAI_NO_INGLES	número inicial da caixa postal 	CHAR(72)
SEPARADOR	@	
PAI_NO_FRANCES	número final da caixa postal	CHAR(72)
SEPARADOR	@	
PAI_ABREVIATURA	número final da caixa postal	CHAR(36)
'''
def importa_pais(file):
    colunas = ['pai_sg','pai_sg_alternativa', 'pai_no_portugues', 'pai_no_ingles', 'pai_no_frances', 'pai_abreviatura']

    dtype = {
        'pai_sg': sqlalchemy.types.CHAR(2),
        'pai_sg_alternativa': sqlalchemy.types.CHAR(3),
        'pai_no_portugues': sqlalchemy.types.CHAR(72),
        'pai_no_ingles': sqlalchemy.types.CHAR(72),
        'pai_no_frances': sqlalchemy.types.CHAR(72),
        'pai_abreviatura': sqlalchemy.types.CHAR(36)
    }

    df = pd.read_csv(file, header=None, index_col='pai_sg', names=colunas, sep='@', encoding='latin1')
    print(df.head())
    salvar(df, 'dne_pais', dtype=dtype)


folder = './tmp/correios/eDNE_Basico_25012/Delimitado'
folder = r'C:\Users\MurilodeMoraesTuvani\tmp\correios\eDNE_Basico_25012\Delimitado'
folder = '/Users/murilotuvani/tmp/ceps/Delimitado'
# folder = '.'
files = os.listdir(folder)

for file in files:
    print(file)
    if file.casefold() == 'LOG_LOCALIDADE.TXT'.casefold():
        importa_localidade(os.path.join(folder, file))
    elif file.casefold() == 'LOG_VAR_LOC.TXT'.casefold():
        importa_localidade_variacao(os.path.join(folder, file))
    elif file.casefold() == 'LOG_FAIXA_LOCALIDADE.TXT'.casefold():
        importa_localidade_faixa(os.path.join(folder, file))
    elif file.casefold() == 'LOG_BAIRRO.TXT'.casefold():
        importa_bairro(os.path.join(folder, file))
    elif file.casefold() == 'LOG_VAR_BAI.TXT'.casefold():
        importa_bairro_variacao(os.path.join(folder, file))
    elif file.casefold() == 'LOG_FAIXA_BAIRRO.TXT'.casefold():
        importa_bairro_faixa(os.path.join(folder, file))
    elif file.casefold().startswith('LOG_LOGRADOURO_'.casefold()):
        importa_logradouro(os.path.join(folder, file))
    elif file.casefold() == 'LOG_FAIXA_CPC.TXT'.casefold():
        importa_caixa_postal_comunitaria_faixa(os.path.join(folder, file))
    elif file.casefold() == 'LOG_CPC.TXT'.casefold():
        importa_caixa_postal_comunitaria(os.path.join(folder, file))
    elif file.casefold() == 'LOG_CPC.TXT'.casefold():
        importa_caixa_postal_comunitaria(os.path.join(folder, file))
    elif file.casefold() == 'LOG_FAIXA_CPC.TXT'.casefold():
        importa_caixa_postal_comunitaria_faixa(os.path.join(folder, file))
    elif file.casefold() == 'LOG_GRANDE_USUARIO.TXT'.casefold():
        importa_grande_usuario(os.path.join(folder, file))
    elif file.casefold() == 'LOG_UNID_OPER.TXT'.casefold():
        importa_unidade_operacional(os.path.join(folder, file))
    elif file.casefold() == 'LOG_FAIXA_UOP.TXT'.casefold():
        importa_unidade_operacional_faixa(os.path.join(folder, file))
    elif file.casefold() == 'LOG_NUM_SEC.TXT'.casefold():
        importa_seccionamento(os.path.join(folder, file))
    elif file.casefold() == 'ECT_PAIS.TXT'.casefold():
        importa_pais(os.path.join(folder, file))
    else:
        print('Not a LOG file {}'.format(file))

