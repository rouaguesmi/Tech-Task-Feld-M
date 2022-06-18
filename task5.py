'''

In order to add support to multiple DBMS, one solution is to create
a class that abstracts the database manipulation,

lets create create a classe : TransactionDBManager : this class will

'''
from os import path
import sqlite3
import psycopg2

HOST_NAME = 'localhost'
DB_NAME = 'transactions'
USERNAME = 'postgres'
PASSWORD = '197382465'
PORT_ID = '5432'

DB_NAME_SQLITE = 'transactions.db'


class TransactionDBManager:
    ''' Abstract database '''
    def __init__(self, dbtype='sqlite') -> None:
        self.m_dbtype = dbtype
        self.m_connection = None
        self.m_cursor = None

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

    def get_connection(self):
        ''' return the connection object '''
        return self.m_connection

    def get_cursor(self):
        '''' Return the cursor object '''
        return self.m_cursor

    def fetchall(self, sql: str):
        ''' Fetch all '''
        if self.m_cursor is not None:
            self.m_cursor.execute(sql)
            return self.m_cursor.fetchall()
        return None

    def fetchone(self, sql: str):
        ''' FetchOne'''
        if self.m_cursor is not None:
            self.m_cursor.execute(sql)
            return self.m_cursor.fetchone()
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

    def is_connected(self) -> bool:
        ''' Write me'''
        return self.m_connection is not None

    def print_visitor_with_most_revenue(self):
        ''' Re-implementing task 1 '''
        query = (''' SELECT visitor_id , SUM(revenue) AS visitor_revenue
                     FROM transactions GROUP BY visitor_id
                     ORDER BY visitor_revenue DESC LIMIT 1''')
        print("The visitor's id having the highest revenue is : ",
              self.fetchone(query)[0])


def task5_solution():
    ''' WRITE ME '''

    transactions_postgres = TransactionDBManager(dbtype='postgres')
    transactions_sqlite = TransactionDBManager(dbtype='sqlite')

    try:

        transactions_postgres.connect(DB_NAME, HOST_NAME,
                                      USERNAME, PASSWORD, PORT_ID)

        sqlite_dbfile_path = path.abspath(DB_NAME_SQLITE)

        transactions_sqlite.connect(sqlite_dbfile_path)

        create_script = ''' DROP TABLE IF EXISTS transactions, devices;

                            CREATE TABLE Devices(
                                id INTEGER,
                                device_name TEXT,
                                PRIMARY KEY(id) );

                            CREATE TABLE Transactions(
                                id SERIAL PRIMARY KEY,
                                datetime TEXT,
                                visitor_id bigint,
                                device_type int,
                                revenue REAL,
                                tax REAL,
                                FOREIGN KEY(device_type)
                                REFERENCES Devices(id));'''

        insert_script_devices = ''' INSERT INTO devices (id, device_name)
                                    VALUES (%s, %s)'''
        insert_script_transactions = ''' INSERT INTO transactions
                                         (id, datetime, visitor_id,
                                         device_type, revenue, tax)
                                         VALUES (%s, %s, %s, %s, %s, %s) '''

        devices = transactions_sqlite.fetchall('SELECT * FROM devices')
        transactions = transactions_sqlite.fetchall(
                                                   'SELECT * FROM transactions'
                                                    )

        transactions_postgres.get_cursor().execute(create_script)
        transactions_postgres.executemany(insert_script_devices, devices)
        transactions_postgres.executemany(insert_script_transactions,
                                          transactions)

        transactions_postgres.commit()

        transactions_postgres.print_visitor_with_most_revenue()
        transactions_sqlite.print_visitor_with_most_revenue()

    except Exception as error:
        print(error)

    finally:
        if transactions_postgres.is_connected():
            transactions_postgres.close()

        if transactions_sqlite.is_connected():
            transactions_sqlite.close()


if __name__ == '__main__':
    task5_solution()
