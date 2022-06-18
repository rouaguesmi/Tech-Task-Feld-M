'''

In order to add support to multiple DBMS, one solution is to create
a class that abstracts the database manipulation,

lets create create a classe : TransactionDBManager : this class will

'''

import sqlite3
import psycopg2

class Transactions:
    ''' Abstract database '''

    def __init__(self, dbtype='sqlite') -> None:
        self.m_dbtype = dbtype
        self.m_connection = None
        self.m_cursor = None
        self.m_devices_name = 'devices'
        self.m_transactions_name= 'transactions'

    def connect(self, dbname, host=None,
                user=None, password=None, port=None):
        ''' Connect to database and create a cursor'''

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

    def execute(self, sql: str):
        ''' todo '''
        if self.m_cursor is not None:
            self.m_cursor.execute(sql)  

    def fetchall(self, sql: str):
        ''' Fetch all '''
        self.execute(sql)
        if self.m_cursor is not None:
            return self.m_cursor.fetchall()
        return None

    def fetchone(self, sql: str):
        ''' FetchOne'''
        self.execute(sql)
        if self.m_cursor is not None:
            return self.m_cursor.fetchone()[0]
        return None

    def executemany(self, sql: str, data: list) -> None:
        ''' ExecuteMany ... '''
        if self.m_cursor is not None:
            self.m_cursor.executemany(sql, data)

    def commit(self):
        ''' Commit the updates to the database'''
        if self.m_connection is not None:
            self.m_connection.commit()

    def close(self) -> None:
        ''' Disconnect from the cursor and the database'''
        if self.m_cursor is not None:
            self.m_cursor.close()
            self.m_cursor = None

        if self.m_connection is not None:
            self.m_connection.close()
            self.m_connection = None

    @property
    def is_connected(self) -> bool:
        ''' Write me'''
        return self.m_connection is not None

    def get_transactions(self):
        """Return all the data from the transactions table"""
        if not self.is_connected:
            raise Exception('Please connect to the database !')
        else:
            sql = f'SELECT * FROM {self.m_transactions_name}'
            return self.fetchall(sql)

    def get_devices(self):
        """Return all the data from the devices table"""
        if not self.is_connected:
            raise Exception('Please connect to the database !')
        else:
            sql = f'SELECT * FROM {self.m_transactions_name}'
            return self.fetchall(sql)


    def print_visitor_with_most_revenue(self):
        ''' Re-implementing task 1 '''
        query = (''' SELECT visitor_id , SUM(revenue) AS visitor_revenue
                     FROM transactions GROUP BY visitor_id
                     ORDER BY visitor_revenue DESC LIMIT 1''')
        print("The visitor's id having the highest revenue is : ",
              self.fetchone(query))
    