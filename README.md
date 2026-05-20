# correios_eDNE_python
Importação dos arquivos fornecidos pelos correios para o banco de dados utilizando Python

## Este projeto não fornece os arquivos, é necessário fazer a aquisição dos arquivos pelo site dos correios.
Após baixar o arquivos coloque em uma pasta qualquer e altere o arquivo config para a linha 'fonte_dados' conter o caminho absoluto da pasta onde constam os arquivos.

#importar_dne_original.py e importar_dne_delta.py
O arquivo (import_dne_atualizado.py) foi criado com o intuito de importar os arquivos, fornecidos em formato TXT pelos Correios, para o servidor no MySQL, a fim de facilitar a higienização, visualização, atualização constante (uma vez que duas vezes ao mês são disponibilizadas atualizações desses logradouros) e manuseio desses dados. 
Primeiramente, como supracitado, faz-se necessário ter os arquivos fornecidos pelos Correios. Em seguida, foi escolhido a pasta 'Delimitado', uma vez que há um separador pré-definido ('@') nos arquivos, facilitando o processo. 
Logo após, foi desenvolvido o arquivo (importar_dne_original.py), criado na intenção de importar o arquivo original (eDNE_Basico_25012), mas serve igualmente para os arquivos das atualizações (tanto já existentes, quanto as futuras), pensando em facilitar e otimizar ao máximo a importação com a implementação de uma função que pode ser chamada por meio de 'importar_dne'. Embora haja uma explicação breve em (importar_dne_original), é necessário colocar algumas questões mais bem explicadas e direcionadas a seguir:

#função para importar as tabelas (deixando mais prático)                                                                 
arquivos_x= []                                -> devem ser colocados os caminhos dos arquivos que serão importados, além de ser necessário nomear o x (de preferência com o conteúdo da tabela)
colunas_x= []                                 -> identifique e nomeie as colunas, colocando-as 'nome_col1', 'nomecol2',...,'nomecoln'
dtype_x= {}                                   -> coloque o tipo de dado (ex.: INTEGER, CHAR, VARCHAR). Definindo e limitando os dados de uma coluna
versoes_x= []                                 -> identifique quais as respectivas atualizações, na ordem em que os arquivos foram colocados

def importar_dne (arquivos, colunas, tabela, dtype, versao):                             -> função                        
    dfs= []                                                                              -> lista que armazenará os dataframes
    for arquivo, versao in zip (arquivos, versoes):                                      -> necessário percorrer cada arquivo citado em 'arquivos_x' e as versões citadas em 'versoes_x'
        df= pd.read_csv(arquivo,                                 
                sep= '@',       
                names= colunas,
                header= None,
                encoding= 'ISO-8859-1'
                )
        df['atualizacao']= versao                                                  -> cria coluna 'atualizacao', para informar de qual atualização o arquivo é proveniente
        dfs.append(df)                                                                   -> adiciona df na lista
    if len(dfs)== 1:                                                                     -> para ver quantos df's existem
        df_final= dfs[0]                                                                 -> se tiver apenas uma tabela, pega o primeiro item
    else:                                                                                -> se houver mais de uma tabela
        df_final= pd.concat(dfs,                                                         -> concatena as tabelas
                    #reorganiza os indices
                    ignore_index= True
                )
    salvar(df_final, tabela, dtype= dtype)                                               -> chama a função salvar, criada no arquivo (importar_dne.py)

Primeiramente, antes de chamar a função, é necessário informar no script: arquivos_x, colunas_x, dtype_x e versoes_x.
Em seguida, use a função importar_dne(arquivos_x, colunas_x, nome_tabela*, dtype_x, versao*); em 'nome_tabela' nomeie a tabela de forma que facilite sua identificação no MySQL.
As atualizações foram colocadas todas em um único arquivo (respeitando, claro, os respectivos conteúdos -bairro, cpc, logradouro etc- mas todas as atualizações de cada um estão no mesmo arquivo, informado por 'dne_delta_x').
Ademais, nos arquivos 'importar_dne_original' e 'importar_dne_delta', foi feita uma mudança na função 'salvar'. Onde antes era 'if_exists= 'replace'', foi trocado por 'append', uma vez que queria todas as atualizações a mostra, não queria que os dados fossem excluídos.s