''' Solution for task 3'''

import csv
from os import path
import sqlite3

FILE_NAME = "tansactions.csv"
DB_NAME = 'transactions.db'


def task3_solution():
    '''Combine the contents of Devices and Transactions
        and store it as a .csv file including the column names.'''

    task3_query = '''   SELECT tr.* , dv.*
                        FROM Transactions tr
                        INNER JOIN devices dv
                        ON dv.id = tr.device_type'''

    # Get absolute path for the file

    file_path = path.abspath(FILE_NAME)

    # Get the absolute path of the database file ("transactions.db")
    dbfile_path = path.abspath(DB_NAME)
    # Connect to the database and create a cursor
    connection = sqlite3.connect(dbfile_path)
    cursor = connection.cursor()

    cursor.execute(task3_query)
    result = cursor.fetchall()

    # Get columns names
    cols_names = (desc[0] for desc in cursor.description)

    # Stream the datas to a csv file
    with open(file_path, 'w', encoding="utf8") as file:
        writer = csv.writer(file)
        writer.writerow(cols_names)
        for row in result:
            writer.writerow(row)


if __name__ == '__main__':
    task3_solution()
