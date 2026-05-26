import pandas as pd
from sqlalchemy import create_engine, text

def atualizar_tabela(tabela_delta, tabela_unificado, pk):
    #conexão com o servidor no MySQL
    engine= create_engine('mysql+pymysql://root:root@10.50.1.252:3306/ceps', pool_recycle=3600)
    #pega versões de atualização
    versoes= pd.read_sql(f"""SELECT DISTINCT atualizacao FROM {tabela_delta} ORDER BY atualizacao """, engine)
    #verifica uma atualização por vez
    for versao in versoes['atualizacao']:
        #pega somente registros daquela atualização 
        delta= pd.read_sql(f""" SELECT * FROM {tabela_delta} WHERE atualizacao = %s """, engine, params=(versao,) )
        #vê se a primary key existe
        if pk not in delta.columns:
             raise ValueError(f"primary key '{pk}' nao encontrada")
        #colunas que não* devem entrar na comparação
        colunas_ignorar= ['ativo', 'data_inicio', 'data_fim', 'atualizacao']
        #pega todas as colunas da tabela (menos as que foram ignoradas acima)
        colunas_banco= pd.read_sql(
             f"SHOW COLUMNS FROM {tabela_unificado}", 
             engine)['Field'].tolist()
        colunas_comparacao= [
            col for col in delta.columns
            if col not in colunas_ignorar
            and col in colunas_banco]
        mudou= False
    #percorre linha por linha
        for linha in delta.itertuples(index=False, name=None):
                #transforma linha em dicionário para ler
                row_data= dict(zip(delta.columns, linha))
                valor_pk= row_data[pk]
                #busca registro ativo atual
                atual= pd.read_sql(f""" SELECT * FROM {tabela_unificado} 
                                    WHERE {pk}= %s AND ativo= 1 """, engine, params=(valor_pk,) )
                nova_linha = pd.DataFrame([row_data])
                nova_linha = nova_linha[[col for col in nova_linha.columns if col in colunas_banco]]
                nova_linha['ativo'] = 1
                nova_linha['data_inicio'] = pd.Timestamp.now()
                nova_linha['data_fim'] = None
                #registro novo
                with engine.begin() as conn:
                    if atual.empty:
                          nova_linha.to_sql(tabela_unificado, conn, if_exists='append', index=False)
                    else:
                         atual = atual.iloc[0]
                         mudou = any(
                               row_data[col] != atual[col]
                               for col in colunas_comparacao)
                    #se o registro mudou
                    if mudou:
                         conn.execute(text(
                              f"""UPDATE {tabela_unificado}
                              SET ativo = 0, data_fim = NOW()
                              WHERE {pk} = :valor_pk AND ativo = 1"""), {"valor_pk": valor_pk})
                         nova_linha.to_sql(tabela_unificado, conn, if_exists='append', index=False)
#obs: nesse caso, vou precisar colocar um drop registros que possuem col 'data_fim' not NULL

#LOCALIDADE
atualizar_tabela('dne_delta_localidade', 'dne_localidade_unificado', 'loc_nu')