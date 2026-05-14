from sqlalchemy import create_engine, Integer, String
import pandas as pd
from urllib.parse import quote
import unicodedata
import re

print("LOGRADOUROS")
arquivo8 = 'C:\\correios\\Fixo\\DNE_DLT_LOGRADOUROS.TXT'
dfl = pd.read_fwf(arquivo8, skiprows= 1,
                            colspecs= [(0, 1), (1, 3), (9, 17), (17, 89), (94, 102), (102, 174), (180, 187), (187, 259), (260, 285), (286, 288), (289, 360), (367, 374), (375, 446), (447, 482), (483, 518), (519, 526), (527, 527), (528, 528)], 
                            dtype= str, 
                            header=None, 
                            encoding='ISO-8859-1'
                )
dfl['arquivo'] = 25012 #precisa acresentar essa coluna; lote arq
#num_colunas4= dfl.shape[1]
#total_linhas4 = dfl.shape[0]

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)

#print(num_colunas4) 
#print(total_linhas4)
print(dfl.count())
print(dfl)