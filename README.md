# correios_eDNE_python
Importação dos arquivos fornecidos pelos correios para o banco de dados utilizando Python

## Este projeto não fornece os arquivos, é necessário fazer a aquisição dos arquivos pelo site dos correios.
Após baixar o arquivos coloque em uma pasta qualquer e altere o arquivo config para a linha 'fonte_dados' conter o caminho absoluto da pasta onde constam os arquivos.

---

## importar_dne_original.py e importar_dne_delta.py
Ambos foram criados com o intuito de importar os arquivos, fornecidos em formato TXT pelos Correios, para o servidor no MySQL. A fim de facilitar a higienização, visualização, atualização constante (uma vez que duas vezes ao mês são disponibilizadas atualizações) e manuseio desses dados.

### informações adicionais
- os arquivos utilizados serão os da pasta 'Delimitado', os quais já possuem um separador pré definido ('@');
- a função 'salvar' provém originalmente do arquivo 'importar_dne.py';
- as funções se encontram logo no início de ambos os scripts;
- nas funções são utilizadas bibliotecas como: Pandas (pd) e sqlalchemy.

### função (def importar_dne)
```
#função para importar as tabelas (deixando mais prático)                                                                 
arquivos_x= []                 # devem ser colocados os caminhos dos arquivos que serão importados, além de ser necessário nomear o x (de preferência com o conteúdo da tabela)
colunas_x= []                  # identifique e nomeie as colunas, colocando-as 'nome_col1', 'nomecol2',...,'nomecoln'
dtype_x= {}                    # coloque o tipo de dado (ex.: INTEGER, CHAR, VARCHAR). Definindo e limitando os dados de uma coluna
versoes_x= []                  # identifique quais as respectivas atualizações, na ordem em que os arquivos foram colocados
def importar_dne (arquivos, colunas, tabela, dtype, versao):              # função                        
    dfs= []                                                               # lista que armazenará os dataframes
    for arquivo, versao in zip (arquivos, versoes):                       # necessário percorrer cada arquivo citado em 'arquivos_x' e as versões citadas em 'versoes_x'
        df= pd.read_csv(arquivo,                                 
                sep= '@',       
                names= colunas,
                header= None,
                encoding= 'ISO-8859-1'
                )
        df['atualizacao']= versao                                          # cria coluna 'atualizacao', para informar de qual atualização o arquivo é proveniente
        dfs.append(df)                                                     # adiciona df na lista
    if len(dfs)== 1:                                                       # para ver quantos df's existem
        df_final= dfs[0]                                                   # se tiver apenas uma tabela, pega o primeiro item
    else:                                                                  # se houver mais de uma tabela
        df_final= pd.concat(dfs,                                           # concatena as tabelas
                    #reorganiza os indices
                    ignore_index= True
                )
    salvar(df_final, tabela, dtype= dtype)                                 # chama a função salvar, modificada nos arquivos importar_dne_original.py e importar_dne_delta.py
```

### como usar?
- insira o caminho dos arquivos em "arquivos_x= []", separando-os por meio de '' (ou "") e ,
- em seguida, coloque o nome das colunas, em "colunas_x= []";
- escreva em "dtype_x= {}" o tipo de dado que estará em cada coluna;
- logo após, insira em "versoes_x= []" o número da atualização dos arquivos, na mesma ordem que estes foram inseridos;
- depois somente se faz necessário chamar a função importar_dne (arquivos_x, colunas_x, 'nome_tabela', dtype_x, versoes_x);
- obs: em 'nome_tabela', nomeie a tabela de modo que facilite sua busca no MySQL;
- acredito ser possível apenas acresentar o arquivo da nova atualização, conforme estas forem sendo disponibilizadas, no 'importar_dne_delta.py' e acrescentar a versão que foi inserida no "versoes_x= []", e analisar as atualizações como um todo no MySQL depois.

---

## atualizar_tabela.py
Função em processo, com o intuito de deixar mais prática a atualização do banco de dados conforme o surgimento de novas. Sabendo que as atualizações vem com frequência (duas vezes ao mês), com o auxílio da função anterior, foram criadas as (dne_x)- as quais são as "tabelas originais"- e foram criadas as (dne_delta_x)- as quais possuem as atualizações, no nosso caso foram utilizadas as atualizações de 25012 à 26031. 

### informações adicionais
- essa segunda função criada ('atualizar_tabela'), necessita primeiramente da criação de uma tabela nova (_unificado), a qual será usada para unificar e comparar as tabelas "originais" de suas "atualizadas", podendo realizar tal ação diretamente na query do MySQL, com os seguintes comandos: 
```         
USE db;
CREATE TABLE dne_x_unificado LIKE dne_x;
ALTER TABLE dne_x_unificado
ADD COLUMN ativo TINYINT DEFAULT 1,
ADD COLUMN data_inicio DATETIME,
ADD COLUMN data_fim DATETIME;
INSERT INTO dne_x_unificado
SELECT *, 1, NOW(), NULL FROM dne_x
```
- a nova tabela (_unificado) é fundamental para que a função funcione, uma vez que a ideia é utilizarmos apenas dela para projetos posteriores;
- as colunas das tabelas (_unificado) e (_delta) precisam estar nomeadas igualmente, caso haja algo inconsistência, tal coluna será "descartada".

### função (def atualizar_tabela)
```
def atualizar_tabela(tabela_delta, tabela_unificado, pk):                                                            # função
    engine= create_engine('mysql+pymysql://root:root@10.50.1.252:3306/ceps', pool_recycle=3600)                      # conexão com o servidor no MySQL
    versoes= pd.read_sql(f"""SELECT DISTINCT atualizacao FROM {tabela_delta} ORDER BY atualizacao """, engine)       # busca valores únicos da coluna 'atualizacao', processará cada um 
    for versao in versoes['atualizacao']:                                                                            # verifica uma atualização de cada vez
        delta= pd.read_sql(f""" SELECT * FROM {tabela_delta} WHERE atualizacao = %s """, engine, params=(versao,) )  # pega somente os registros daquela atualização
        if pk not in delta.columns:                                                                                  # verifica se a primary key existe
             raise ValueError(f"primary key '{pk}' nao encontrada")                                                  
        colunas_ignorar= ['ativo', 'data_inicio', 'data_fim', 'atualizacao']                                         # colunas as quais não devem ser comparadas
        colunas_banco= pd.read_sql(                                                                                  # pega todas as colunas da tabela (menos as que foram "ignoradas")
             f"SHOW COLUMNS FROM {tabela_unificado}", 
             engine)['Field'].tolist()
        colunas_comparacao= [                                                                                        # define quais colunas serão comparadas:
            col for col in delta.columns                                                                             # existem no delta
            if col not in colunas_ignorar                                                                            # não são "ignoradas"
            and col in colunas_banco]                                                                                # existem no banco
        mudou= False
        for linha in delta.itertuples(index=False, name=None):                                                       # processa cada linha individulamente
                row_data= dict(zip(delta.columns, linha))                                                            # facilita acessar valores pelo nome da coluna
                valor_pk= row_data[pk]                                                                               # pega o valor da primary key
                atual= pd.read_sql(f""" SELECT * FROM {tabela_unificado}                                             # vê se o registro já está na tabela (_unificado)
                                    WHERE {pk}= %s AND ativo= 1 """, engine, params=(valor_pk,) )
                nova_linha = pd.DataFrame([row_data])                                                                # cria uma nova linha para inserir e prepara-a com as informações:
                nova_linha = nova_linha[[col for col in nova_linha.columns if col in colunas_banco]]                 # mantém somente as colunas válidas    
                nova_linha['ativo'] = 1                                                                              # adiciona "ativo" facilitando a visualização
                nova_linha['data_inicio'] = pd.Timestamp.now()                                                       # quando tal atualização entrou em "vigor"
                nova_linha['data_fim'] = None                                                                        # deixa de ser NULL quando a info. é atualizada
                with engine.begin() as conn:                                                                         # garante que tudo é salvo 
                    if atual.empty:                                                                                  # se não existe:
                          nova_linha.to_sql(tabela_unificado, conn, if_exists='append', index=False)                 # é um registro novo, necessário inserir apenas
                    else:                                                                                            # se já existe:
                         atual = atual.iloc[0]                                                                       # pega o registro atual e compara
                         mudou = any(
                               row_data[col] != atual[col]
                               for col in colunas_comparacao)                                                        # verifica se alguma coluna mudou
                    if mudou:                                                                                        # se mudou:
                         conn.execute(text(
                              f"""UPDATE {tabela_unificado}
                              SET ativo = 0, data_fim = NOW()                                                        # desativa o antigo e deixa registrado 'data_fim'
                              WHERE {pk} = :valor_pk AND ativo = 1"""), {"valor_pk": valor_pk})                      
                         nova_linha.to_sql(tabela_unificado, conn, if_exists='append', index=False)                  # insere o novo registro (ativo)
```

### como usar?
- primeiramente, é necessário informar a conexão correta com o servidor no MySQL;
- logo após, faz-se necessário ter as tabelas (_delta) e (_unificado);
- em seguida, basta chamar a função 'atualizar_tabela';
- informe, dentro dos parenteses (), os nomes dados respectivamente à: tabela_delta, tabela_unificado e pk;
- por enquanto esta função aceita somente tabelas com primary keys simples (as tabelas com pk compostas, são aquelas de variação e/ou faixa).