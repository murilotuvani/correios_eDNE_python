# correios_eDNE_python
Importação dos arquivos fornecidos pelos correios para o banco de dados utilizando Python

## Este projeto não fornece os arquivos, é necessário fazer a aquisição dos arquivos pelo site dos correios.
Após baixar o arquivos coloque em uma pasta qualquer e altere o arquivo config para a linha 'fonte_dados' conter o caminho absoluto da pasta onde constam os arquivos.



## importar_dne_original.py e importar_dne_delta.py
Ambos foram criados com o intuito de importar os arquivos, fornecidos em formato TXT pelos Correios, para o servidos no MySQL. A fim de facilitar a higienização, visualização, atualização constante (uma vez que duas vezes ao mês são disponibilizadas atualizações) e manuseio desses dados.

### informações adicionais
- os arquivos utilizados serão os da pasta 'Delimitado', os quais já possuem um separador pré definido ('@');
- a função 'salvar' provém originalmente do arquivo 'importar_dne.py', mas passou por mudança no "if_exists= 'replace'" para "if_exists= 'apend'" ao ser colocada nos arquivos de importação;
- as funções se encontram logo no início de ambos os scripts;
- nas funções são utilizadas bibliotecas como: Pandas (pd) e sqlalchemy.

### função (def importar_dne)
#função para importar as tabelas (deixando mais prático)                                                                 
arquivos_x= []                 -> devem ser colocados os caminhos dos arquivos que serão importados, além de ser necessário nomear o x (de preferência com o conteúdo da tabela)
colunas_x= []                  -> identifique e nomeie as colunas, colocando-as 'nome_col1', 'nomecol2',...,'nomecoln'
dtype_x= {}                    -> coloque o tipo de dado (ex.: INTEGER, CHAR, VARCHAR). Definindo e limitando os dados de uma coluna
versoes_x= []                  -> identifique quais as respectivas atualizações, na ordem em que os arquivos foram colocados

def importar_dne (arquivos, colunas, tabela, dtype, versao):              -> função                        
    dfs= []                                                               -> lista que armazenará os dataframes
    for arquivo, versao in zip (arquivos, versoes):                       -> necessário percorrer cada arquivo citado em 'arquivos_x' e as versões citadas em 'versoes_x'
        df= pd.read_csv(arquivo,                                 
                sep= '@',       
                names= colunas,
                header= None,
                encoding= 'ISO-8859-1'
                )
        df['atualizacao']= versao                                          -> cria coluna 'atualizacao', para informar de qual atualização o arquivo é proveniente
        dfs.append(df)                                                     -> adiciona df na lista
    if len(dfs)== 1:                                                       -> para ver quantos df's existem
        df_final= dfs[0]                                                   -> se tiver apenas uma tabela, pega o primeiro item
    else:                                                                  -> se houver mais de uma tabela
        df_final= pd.concat(dfs,                                           -> concatena as tabelas
                    #reorganiza os indices
                    ignore_index= True
                )
    salvar(df_final, tabela, dtype= dtype)                                 -> chama a função salvar, modificada nos arquivos importar_dne_original.py e importar_dne_delta.py

### como usar?
- insira o caminho dos arquivos em "arquivos_x= []", separando-os por meio de '' (ou "") e ,
- em seguida, coloque o nome das colunas, em "colunas_x= []";
- escreva em "dtype_x= {}" o tipo de dado que estará em cada coluna;
- logo após, insira em "versoes_x= []" o número da atualização dos arquivos, na mesma ordem que estes foram inseridos;
- depois somente se faz necessário chamar a função importar_dne (arquivos_x, colunas_x, 'nome_tabela', dtype_x, versoes_x);
- obs: em 'nome_tabela', nomeie a tabela de modo que facilite sua busca no MySQL;
- acredito ser possível apenas acresentar o arquivo da nova atualização, conforme estas forem sendo disponibilizadas, no 'importar_dne_delta.py' e acrescentar a versão que foi inserida no "versoes_x= []", e analisar as atualizações como um todo no MySQL depois.