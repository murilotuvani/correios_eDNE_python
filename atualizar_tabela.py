import pandas as pd
from sqlalchemy import create_engine, text

def atualizar_tabela(tabela_delta, tabela_unificado, pk):                                                           
    engine= create_engine('mysql+pymysql://root:root@10.50.1.252:3306/ceps', pool_recycle=3600)                   
    versoes= pd.read_sql(f"""SELECT DISTINCT atualizacao FROM {tabela_delta} ORDER BY atualizacao """, engine)      
    for versao in versoes['atualizacao']:                                                                       
        delta= pd.read_sql(f""" SELECT * FROM {tabela_delta} WHERE atualizacao = %s """, engine, params=(versao,) ) 
        if pk not in delta.columns:                                                                               
             raise ValueError(f"primary key '{pk}' nao encontrada")   
        colunas_ignorar= ['ativo', 'data_inicio', 'data_fim', 'atualizacao']                                         
        colunas_banco= pd.read_sql(                                                                                  
             f"SHOW COLUMNS FROM {tabela_unificado}", 
             engine)['Field'].tolist()                                               
        colunas_comparacao= [                                                                               
            col for col in delta.columns                                                                        
            if col not in colunas_ignorar                                                                  
            and col in colunas_banco]                                                                            
        mudou= False
        for linha in delta.itertuples(index=False, name=None):                                                 
                row_data= dict(zip(delta.columns, linha))                                                           
                valor_pk= row_data[pk]                                                                               
                atual= pd.read_sql(f""" SELECT * FROM {tabela_unificado}                                             
                                    WHERE {pk}= %s AND ativo= 1 """, engine, params=(valor_pk,) )
                nova_linha = {
                     col: (None if pd.isna(row_data[col]) else row_data[col])
                     for col in row_data
                     if col in colunas_banco}                                                                                                                               
                with engine.begin() as conn:                                                                         
                    if atual.empty:                                                                                  
                          df_insert = pd.DataFrame([nova_linha])
                          df_insert.to_sql(tabela_unificado, conn, if_exists='append', index=False)                
                    else:                                                                                            
                         atual = atual.iloc[0]
                         mudou = any(
                              row_data[col] != atual[col]
                              for col in colunas_comparacao)                                                        
                    if mudou:
                         set_clause = ", ".join([f"{col} = :{col}" for col in nova_linha.keys()])
                         conn.execute(
                              text(f"""UPDATE {tabela_unificado}
                                   SET {set_clause}
                                   WHERE {pk} = :{pk}"""),
                                   nova_linha)                 
#obs: nesse caso, vou precisar colocar um drop registros que possuem col 'data_fim' not NULL

#LOCALIDADE
atualizar_tabela('dne_delta_localidade', 'dne_localidade_unificado', 'loc_nu')

atualizar_tabela('dne_delta_logradouro', 'dne_logradouro_unificado', 'log_nu')