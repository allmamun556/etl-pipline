import psycopg2
import pandas as pd
from sqlalchemy import create_engine
import transform
import db
import configparser 
from configparser import ConfigParser


configur=ConfigParser()
configur.read("C:\\Users\\z5iksxt\\project\\ETL pipeline\etl\\configfile.ini")
#print ("Sections : ", configur.sections())
user=configur.get('DB','user')
host=configur.get('DB','host')
password=configur.get('DB','password')
dbname=configur.get('DataBase','dbname')



#username: postgres
#password: Htmpzkvm7867
#host:localhost
#database name: datalake_final

conn_string = "postgresql://{}:{}@{}/{}".format(user, password, host, dbname)



#conn_string = "postgresql://postgres:Htmpzkvm7867@localhost/datalake_final"
  
db_sql = create_engine(conn_string)
conn = db_sql.connect()

data=db.db_load()
data=transform.convert_us_to_gbp(data,['BTC','ETH','XRP','LTC'])
data=transform.required_column(data)
#print('See the data')
#print(data)
# Create DataFrame
#df = pd.DataFrame(data)
def load_to_db(table_name, connection, dataframe):

    dataframe.to_sql(table_name, con=connection, if_exists='replace',
            index=False)
    connection = psycopg2.connect(conn_string
                            )
    connection.autocommit = True
    #cursor = connection.cursor()
    
    #sql1 = '''select * from table_name;'''
    #cursor.execute(sql1)
    #for i in cursor.fetchall():
        #print(i)
    
    # conn.commit()
    connection.close()


load_to_db('currency_table',conn,data)    





def read_from_db(sql_query):
    conn = psycopg2.connect(conn_string)

    #Setting auto commit false
    conn.autocommit = True

    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    #Retrieving data
    cursor.execute(sql_query)

    #Fetching 1st row from the table
    result = cursor.fetchone();
    print(result)

    #Fetching 1st row from the table
    result = cursor.fetchall();
    print(result)

    #Commit your changes in the database
    conn.commit()

    #Closing the connection
    conn.close()

#read_from_db('''SELECT * from currency_table''')    



import configparser
from configparser import ConfigParser

#cfg = ConfigParser()
#cfg.read('configfile.ini')

#print ("Sections : ", cfg.sections())

