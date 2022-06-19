'''Solution for task 3'''

import csv
from os import path
import sqlite3

CSV_FILE_NAME = "transactions.csv"
DB_NAME = 'transactions.db'


def task3_solution():
    '''Combine the contents of Devices and Transactions
        and store it as a .csv file including the column names.'''

    task3_query = '''   SELECT tr.* , dv.*
                        FROM Transactions tr
                        INNER JOIN devices dv
                        ON dv.id = tr.device_type'''

    # Get absolute path for the file
    csv_file_path = path.abspath(CSV_FILE_NAME)

    # Get the absolute path of the database file ("transactions.db")
    dbfile_path = path.abspath(DB_NAME)

    connection = None
    transactions = []

    try:
        # Connect to the database and create a cursor
        with sqlite3.connect(dbfile_path) as connection:
            with connection.cursor() as cursor:
                cursor.execute(task3_query)
                transactions = cursor.fetchall()
                # Get columns names
                cols_names = (desc[0] for desc in cursor.description)
        
        # Stream the fetched datas to a csv file
        with open(csv_file_path, 'w', encoding="utf8") as file:
            writer = csv.writer(file)
            writer.writerow(cols_names)
            for row in transactions:
                writer.writerow(row)

    except Exception as error:
        if connection is not None:
            connection.close()


if __name__ == '__main__':
    task3_solution()
