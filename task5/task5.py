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
    ''' Test the transactions class :
    1/ Copy the transactions sqlite database into a postgres database
    2/ Answer task 1 using the transactions class with postgres and sqlite
    '''

    transactions_postgres = tr.TransactionsDBManager(dbtype='postgres')
    transactions_sqlite = tr.TransactionsDBManager(dbtype='sqlite')

    try:

        transactions_postgres.connect(DB_NAME, HOST_NAME,
                                      USERNAME, PASSWORD, PORT_ID)

        sqlite_dbfile_abspath = path.abspath(DB_NAME_SQLITE)

        transactions_sqlite.connect(sqlite_dbfile_abspath)

        
        create_script = ''' DROP TABLE IF EXISTS transactions, devices;

                            CREATE TABLE Devices(
                                id INTEGER,
                                device_name TEXT,
                                PRIMARY KEY(id) );

                            CREATE TABLE Transactions(
                                id INTEGER PRIMARY KEY,
                                datetime TEXT,
                                visitor_id BIGINT,
                                device_type INTEGER,
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

        devices = transactions_sqlite.get_devices()
        transactions = transactions_sqlite.get_transactions()

        transactions_postgres.execute(create_script)

        transactions_postgres.executemany(insert_script_devices, devices)
        transactions_postgres.executemany(insert_script_transactions, transactions)

        transactions_postgres.commit()
        
        transactions_postgres.print_visitor_with_most_revenue()
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
