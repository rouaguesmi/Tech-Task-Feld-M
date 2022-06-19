'''

In order to add support to multiple DBMS, one solution is to create
a class that abstracts the database manipulation,

lets create create a classe : TransactionDBManager : this class will

'''

import sqlite3
import psycopg2

# Error message to be displayed when the database is disconnected.
DB_DISCONNECTED_MSG = 'Database is disconnected.'

class TransactionsDBManager:
    ''' Class to manage the 'Transactions' database using different 
        databases management systems (DBMS). Currently, it supports 
        SQLite and PostgreSQL DBMS. 
        But it is easily upgradable to supprot other DBMS which
        have python libraries that implement the
        PEP 248 (python database API specifications v2.0)'''

    def __init__(self, dbtype='sqlite') -> None:
        self.m_dbtype = dbtype
        self.m_connection = None
        self.m_cursor = None
        self.m_devices_name = 'devices'
        self.m_transactions_name= 'transactions'

    def connect(self, dbname, host=None,
                user=None, password=None, port=None):
        '''Connect to the database and create a cursor object.'''

        if self.m_dbtype == 'sqlite':
            self.m_connection = sqlite3.connect(dbname)

        elif self.m_dbtype == 'postgres':
            self.m_connection = psycopg2.connect(
                                    host=host,
                                    dbname=dbname,
                                    user=user,
                                    password=password,
                                    port=port)

        self.m_cursor = self.m_connection.cursor()

    def execute(self, sql: str) -> None:
        '''Execute a sql query '''
        if self.m_cursor is not None:
            self.m_cursor.execute(sql)
        else:
            raise Exception(DB_DISCONNECTED_MSG)

    def fetchall(self, sql: str) -> list:
        '''Fetch all data returned by the sql query execution.'''
        self.execute(sql)
        if self.m_cursor is not None:
            return self.m_cursor.fetchall()
        else:
            raise Exception(DB_DISCONNECTED_MSG)

    def fetchone(self, sql: str):
        ''' Fetch one'''
        self.execute(sql)
        if self.m_cursor is not None:
            return self.m_cursor.fetchone()[0]
        else:
            raise Exception(DB_DISCONNECTED_MSG)

    def executemany(self, sql: str, data=None) -> None:
        ''' ExecuteMany ... '''
        if self.m_cursor is not None:
            self.m_cursor.executemany(sql, data)
        else:
            raise Exception(DB_DISCONNECTED_MSG)

    def commit(self) -> None:
        ''' Commit the updates to the database'''
        if self.m_connection is not None:
            self.m_connection.commit()
            print('Commit to the database done.')
        else:
            print(DB_DISCONNECTED_MSG)

    def close(self) -> None:
        ''' Disconnect from the cursor and the database'''
        if self.m_cursor is not None:
            self.m_cursor.close()
            self.m_cursor = None

        if self.m_connection is not None:
            self.m_connection.close()
            self.m_connection = None
            print("Connection closed.")
        else:
            print('Connection is already closed.')

    @property
    def is_connected(self) -> bool:
        ''' Return true if the database is connected, false otherwise '''
        return self.m_connection is not None

    def get_transactions(self) -> list:
        """Return all the data from the transactions table"""
        if not self.is_connected:
            raise Exception(DB_DISCONNECTED_MSG)
        else:
            sql = f'SELECT * FROM {self.m_transactions_name}'
            return self.fetchall(sql)

    def get_devices(self) -> list:
        """Return all the data from the devices table"""
        if not self.is_connected:
            raise Exception(DB_DISCONNECTED_MSG)
        else:
            sql = f'SELECT * FROM {self.m_transactions_name}'
            return self.fetchall(sql)

    def print_visitor_with_most_revenue(self):
        ''' Re-implementing task 1 '''
        query = (''' SELECT visitor_id , SUM(revenue) AS visitor_revenue
                     FROM transactions GROUP BY visitor_id
                     ORDER BY visitor_revenue DESC''')
        print("The visitor's id generating the highest revenue is : ",
              self.fetchone(query))
    