'''

In order to add support to multiple DBMS, one solution is to create 
a class that abstracts the database manipulation,

lets create create a classe : TransactionDBManager : this class will 

'''

import sqlite3

class TransactionDBManager:

    def __init__(self, db_type='sqlite') -> None:
        self.db_type = db_type
        self.connection = None
        self.cursor = None


    def connect_to_db(self, table):
        ''' Connect to database and create a cursor'''
        if self.db_type == 'sqlite':
            self.connection = sqlite3.connect(table)
            self.cursor = sqlite3.Cursor()
        elif self.db_type == 'PostgreSql'
            self.connection = 
            self.cursor = 
        elif self.db_type == 'Mysql':
            self.connection = 
            self.cursor = 

    def execute_query(self, sql:str):
        ''' ex'''



SQLITE_DB_MANAGER = TransactionDBManager('sqlite')

SQLITE_DB_MANAGER.execute_query('fffff')

POSGRESQLI_DB_MANAGER = SQLITE_DB_MANAGER('PosGreSQLI')

POSGRESQLI_DB_MANAGER.execute_query('fffff')
