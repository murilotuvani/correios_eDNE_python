import pandas as pd
import pymysql
from sqlalchemy import create_engine


file_path = r"C:\Users\MurilodeMoraesTuvani\Downloads\202502bairros.xlsx"
file_path = r"/Users/murilotuvani/Downloads/202502bairros.xlsx"
file_path = r"C:\Users\Murilo\Downloads\202502bairros.xlsx"
df = pd.read_excel(file_path)

print(df.head())
print(df.shape)

tableName = 'rotas_externas_bairros'
sqlEngine       = create_engine('mysql+pymysql://root:root@127.0.0.1:3306/cep', pool_recycle=3600)
dbConnection    = sqlEngine.connect()
transaction = dbConnection.begin()

try:
    frame           = df.to_sql(tableName, dbConnection, if_exists='replace');
    transaction.commit()
except ValueError as vx:
    print(vx)
except Exception as ex:
    print(ex)
else:
    print("Table %s created successfully."%tableName);   
finally:
    dbConnection.close()