import pandas as pd
from sqlalchemy import create_engine

def atualizar_tabela(tabela_delta, tabela_unificado, pk, coluna_comparacao):
    #configurar conexão com o servidor no MySQL
    engine= create_engine('mysql+pymysql://root:root@10.50.1.252:3306/ceps', pool_recycle=3600)
    # pega todas as atualizações diferentes e coloca em ordem crescente
    versoes= pd.read_sql(f"SELECT DISTINCT atualizacao FROM {tabela_delta} ORDER BY atualizacao", 
                         engine)
    for versao in versoes['atualizacao']:
        print(f"\natualizacao: {versao}")
        #pedimos ao banco somente as linhas daquela atualização
        delta= pd.read_sql(f"SELECT * FROM {tabela_delta} WHERE atualizacao = %s",
                           engine,
                           params=(versao,))
        if pk not in delta.columns:
            raise ValueError(f"primary key column '{pk}' nao encontrada")
        #cada linha do delta vira uma tupla simples; mais rápido que iterrows (usei antes)
        for linha in delta.itertuples(index=False, name=None):
            #transforma a linha em um dicionário para achar os valores pelo nome da coluna (Copilot)
            row_data= dict(zip(delta.columns, linha))
            valor_pk= row_data[pk]
            #verifica se já existe um registro com essa primary key na tabela unificada
            atual= pd.read_sql(f"SELECT * FROM {tabela_unificado} WHERE {pk} = %s",
                                engine,
                                params=(valor_pk,))
            if atual.empty:
                #se não existir, insere um novo registro e marca como ativo
                nova_linha= pd.DataFrame([row_data])
                nova_linha['ativo']= 1
                nova_linha['data_inicio']= pd.Timestamp.now()
                nova_linha['data_fim']= None
                nova_linha.to_sql(tabela_unificado, engine, if_exists='append', index=False)
                print('novo registro')
            else:
                #se existir, pega a primeira linha encontrada
                atual= atual.iloc[0]
                # compara a coluna que interessa para saber se mudou
                mudou= row_data[coluna_comparacao] != atual[coluna_comparacao]

                if mudou:
                    #se mudou, desativa o registro antigo
                    engine.execute((f"UPDATE {tabela_unificado} SET ativo = 0, data_fim = NOW() " 
                                    f"WHERE {pk} = :valor_pk AND ativo = 1"),
                                    {'valor_pk': valor_pk})
                    #insere a nova versão do registro
                    nova_linha= pd.DataFrame([row_data])
                    nova_linha['ativo']= 1
                    nova_linha['data_inicio']= pd.Timestamp.now()
                    nova_linha['data_fim']= None
                    nova_linha.to_sql(tabela_unificado, engine, if_exists='append', index=False)
                    print('registro atualizado')


#LOCALIDADE
pk= ['loc_nu']
coluna_comparacao= ['loc_no']
atualizar_tabela('dne_delta_localidade', 'dne_localidade_unificado', 'loc_nu', 'loc_no' )