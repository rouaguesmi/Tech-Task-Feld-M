'''

In order to add support to multiple DBMS, one solution is to create
a class that abstracts the database manipulation,

lets create create a classe : TransactionDBManager : this class will

'''

import sqlite3
import psycopg2

HOST_NAME = 'localhost'
DB_NAME = 'transactions'
USERNAME = 'postgres'
PASSWORD = '197382465'
PORT_ID = '5432'


class TransactionDBManager:
    ''' Abstract database '''
    def __init__(self, dbtype='sqlite') -> None:
        self.m_dbtype = dbtype
        self.m_connection = None
        self.m_cursor = None

    def connect_to_db(self, dbname, host=None,
                      user=None, password=None, port=None):
        ''' Connect to database and create a cursor'''
        if self.m_dbtype == 'sqlite':
            self.m_connection = sqlite3.connect(dbname)
        elif self.m_dbtype == 'PostgreSql':
            self.m_connection = psycopg2.connect(
                                    host=host,
                                    dbname=dbname,
                                    user=user,
                                    password=password,
                                    port=port)

    def execute_query(self, sql: str):
        ''' ex'''
        pass
    

connection = psycopg2.connect(
                host=HOST_NAME,
                dbname=DB_NAME,
                user=USERNAME,
                password=PASSWORD,
                port=PORT_ID)
cursor = connection.cursor()

create_script = ''' CREATE TABLE Devices(
                        id INTEGER,
                        device_name TEXT,
                        PRIMARY KEY(id) );
                                                              
                    CREATE TABLE Transactions(
                        id SERIAL PRIMARY KEY,
                        datetime INTEGER,
                        visitor_id INTEGER,
                        device_type INTEGER,
                        revenue REAL,
                        tax REAL,
                        FOREIGN KEY(device_type) REFERENCES Devices(id)); '''
                        
insert_script = ''' INSERT INTO devices 
'''
cursor.execute(create_script)
connection.commit()

cursor.close()
connection.close()




