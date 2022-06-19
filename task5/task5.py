'''Solution for task 5'''

from os import path
import transactions as tr

# Credentials to connect to postgesql server:
# Please update these parameters according to your server configuration.

HOST_NAME = 'localhost'
DB_NAME = 'transactions'
USERNAME = 'postgres'
PASSWORD = '197382465'
PORT_ID = '5432'

DB_NAME_SQLITE = 'transactions.db'


def task5_solution():
    '''
    In order to demostrate the utility of the class TransactionsDBManager
    I will use it to do the following tasks :

    1/ Copy the transactions sqlite database into a postgres database.
    2/ solve task 1 with postgresql and sqlite databases using two
    objects of the class TransactionsDBManager

    Please refer to the README file or the transactions.py
    module for more information about the TransactionsDBManager class
    '''

    # the transactions_postgres object will manage the transactions DB
    #  in a PostgreSQL database server.
    transactions_postgres = tr.TransactionsDBManager(dbtype='postgres')
    # the transactions_sqlite object will manage the transactions DB
    #  in a SQLite database server.
    transactions_sqlite = tr.TransactionsDBManager(dbtype='sqlite')

    try:
        # Connect to the databse :
        transactions_postgres.connect(DB_NAME, HOST_NAME,
                                      USERNAME, PASSWORD, PORT_ID)
        sqlite_dbfile_abspath = path.abspath(DB_NAME_SQLITE)
        transactions_sqlite.connect(sqlite_dbfile_abspath)

        # Sql script to create the tables devices and transactions
        # in the postgres transactions database.
        # The database 'transactions' should already be created in the server.
        create_script = '''
                        DROP TABLE IF EXISTS transactions, devices;

                        CREATE TABLE Devices(
                            id INTEGER,
                            device_name TEXT,
                            PRIMARY KEY(id) );

                        CREATE TABLE Transactions(
                            id INTEGER PRIMARY KEY,
                            datetime TIMESTAMP,
                            visitor_id BIGINT,
                            device_type INTEGER,
                            revenue REAL,
                            tax REAL,
                            FOREIGN KEY(device_type)
                            REFERENCES Devices(id));
                        '''

        # Sql scripts to insert the data the newly created tables
        insert_devices = ''' INSERT INTO devices (id, device_name)
                                    VALUES (%s, %s)'''
        insert_transactions = ''' INSERT INTO transactions
                                         (id, datetime, visitor_id,
                                         device_type, revenue, tax)
                                         VALUES (%s, %s, %s, %s, %s, %s) '''

        # Get devices and transactions tables data from the sqlite database.
        devices = transactions_sqlite.get_devices()
        transactions = transactions_sqlite.get_transactions()

        # Execute scripts tables creations and data insertion
        # to the postgreSQL Database :
        transactions_postgres.execute(create_script)
        transactions_postgres.executemany(insert_devices, devices)
        transactions_postgres.executemany(insert_transactions, transactions)
        # Commit the changes.
        transactions_postgres.commit()

        # Answer task using a PostgreSQL database server :
        transactions_postgres.print_visitor_with_most_revenue()
        # Answer task using a SQLite database server :
        transactions_sqlite.print_visitor_with_most_revenue()

    except Exception as error:
        print(error)

    finally:
        if transactions_postgres.is_connected:
            transactions_postgres.close()

        if transactions_sqlite.is_connected:
            transactions_sqlite.close()


if __name__ == '__main__':
    task5_solution()
