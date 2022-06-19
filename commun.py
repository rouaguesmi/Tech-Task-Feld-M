''' Commun module contains commun functions used in
    tasks 1, 2, 3 and 4 '''

import sqlite3
from os import path
import xml.etree.ElementTree as ET

DB_NAME = 'transactions.db'
EUROFXREF_XML = 'eurofxref-hist-90d.xml'

NO_RESULT_STR = 'No result was found in the database.'

def connect_and_execute_query(sql_query: str) -> list:
    """ Connect to the sqlite database 'transactions.db', 
        execute the query 'sql_query' and return the result"""

    # Get the absolute path of the database file ("transactions.db")
    dbfile_path = path.abspath(DB_NAME)

    connection = None
    result = []
    try:
        # Connect to the database and create a cursor
        with sqlite3.connect(dbfile_path) as connection:
            with connection.cursor() as cursor:
                # Execute the query
                cursor.execute(sql_query)
                # Fetch the returned result
                result = cursor.fetchall()
    except Exception as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

    return result


def extract_exchange_rates(file=EUROFXREF_XML, currency='USD') -> map:
    ''' write me '''

    xml_file_path = path.abspath(file)
    tree = ET.parse(xml_file_path)
    root = tree.getroot()
    namespaces = {"ex": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}
    currency_rates = {}
    for cube in root.findall(".//ex:Cube[@time]", namespaces=namespaces):
        for child in cube.iter():
            if child.get('currency') == currency:
                currency_rates[cube.get('time')] = float(child.get('rate'))

    return currency_rates
