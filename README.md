# correios_eDNE_python
Importação dos arquivos fornecidos pelos correios para o banco de dados utilizando Python

## Este projeto não fornece os arquivos, é necessário fazer a aquisição dos arquivos pelo site dos correios.
Após baixar o arquivos coloque em uma pasta qualquer e altere o arquivo config para a linha 'fonte_dados' conter o caminho absoluto da pasta onde constam os arquivos.

#05/2026
O arquivo 'import_dne_atualizado.py' foi criado com o intuito de importar os arquivos, fornecidos em formato TXT pelos Correios, para o servidor no MySQL, a fim de facilitar a higienização, visualização, atualização constante (uma vez que duas vezes ao mês são disponibilizadas atualizações desses logradouros) e manuseio desses dados. 
Primeiramente, como supracitado, faz-se necessário ter os arquivos fornecidos pelos Correios. Em seguida, foi escolhido a pasta 'Delimitado', uma vez que há um separador pré-definido ('@') nos arquivos, facilitando o processo. 
Logo após, foi criado o arquivo 'import_dne_atualizado.py', o qual faz uso principalmente das bibliotecas pandas (importado como 'pd') e sqlalchemy. Além do uso da função 'salvar', a qual também está presente em 'correios_eDNE_python', no arquivo denominado 'importar_dne.py'.
A estrutura de todo o código é semelhante entre si, já que o objetivo fora importar os arquivos para o MySQL utilizando pandas. As únicas mudanças ao longo dele foram as nomeações dadas para os arquivos e tabelas, para não haver confusão no processamento. Cada arquivo importado tem início em #*NOME DO ARQUIVO* e fim na sua respectiva função 'salvar'. 

Explicando o código:
#*NOME DO ARQUIVO*                                           -> um breve comentário sobre qual será a informação importada
arquivoX = 'C:\\correios\\Delimitado\\DELTA_LOG_ZZZ.TXT'     -> variável 'arquivoX' para facilitar chamar o arquivo no pd.read_csv
dtypeX = {                                                   
        'name_1' = *tipo de dado*,
        'name_2' = *tipo de dado*,                           
           .                                                 -> colocando o tipo de dado (ex.: INTEGER, CHAR, VARCHAR). Definindo e limitando os dados de uma coluna
           .
           .
        'name_n' = *tipo de dado*
     }
dfX = pd.read_csv(arquivoX, skiprows= 1,                     -> leitura do arquivo, utilizando pandas. *1 
                            sep= '@',                        -> separador utilizado nos arquivos recebidos
                            names= ['name_1',...,'name_n'],  -> nomes dados as colunas, respectivamente (provenientes do arquivo '\\Delimitado\\Leiautes_Delta_delimitador.doc')
                            header=None,    
                            encoding='ISO-8859-1'        
                )
dfX['arquivo'] = 25012                                       -> coluna acrescentada, para representar qual a atualização referente aquela informação
salvar(dfX, 'dne_atualiza_X', dtype= dtypeX)                 -> função 'salvar', proveniente do arquivo 'importar_dne.py'

obs*: 1- Ao disponibilizar os arquivos, os Correios colocaram um cabeçalho, portanto foi necessário pular a primeira linha (skiprows=1)

