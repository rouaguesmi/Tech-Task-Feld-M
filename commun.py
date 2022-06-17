''' Commun utility for the tasks '''

import sqlite3
from os import path

DB_NAME = 'transactions.db'

def connect_and_execute_query(sql_query: str) -> list:
    """ Connect to the sqlite database, execute the query 'sql_query'
     and return the result"""

    # Get the absolute path of the database file ("transactions.db")
    dbfile_path = path.abspath(DB_NAME)
    # Connect to the database and create a cursor
    connection = sqlite3.connect(dbfile_path)
    cursor = connection.cursor()
    # Execute the query
    cursor.execute(sql_query)
    # Fetch the returned result
    result = cursor.fetchall()
    # Close the connection and return
    connection.close()
    return result
