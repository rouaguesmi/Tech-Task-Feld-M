''' Utils module contains commun functions used in
    tasks 1 to 4 '''

import sqlite3
from os import path
import xml.etree.ElementTree as ET
from datetime import datetime

DB_NAME = 'transactions.db'
EUROFXREF_XML = 'eurofxref-hist-90d.xml'

NO_RESULT_STR = 'No result was found in the database.'


def connect_and_execute_query(sql_query: str) -> list:
    """ Connect to the sqlite database 'transactions.db',
        execute the query 'sql_query' and return the result """

    # Get the absolute path of the sqlite database file ("transactions.db")
    dbfile_path = path.abspath(DB_NAME)

    connection = None
    result = []
    try:
        # Connect to the database and create a cursor
        with sqlite3.connect(dbfile_path) as connection:
            cursor = connection.cursor()
            # Execute the sql query
            cursor.execute(sql_query)
            # Fetch the returned result
            result = cursor.fetchall()
    except sqlite3.Error as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

    return result


def extract_exchange_rates(file=EUROFXREF_XML, currency='USD') -> dict:
    '''
    Extract the exchange rates of the currency mentioned in the parameters from
    the provided XML file and return a dictionary containing the date as a key
    and the exchange rate as a corresponding value.
    '''

    # Get the absolute path of the XML file ("eurofxref-hist-90d.xml")
    xml_file_path = path.abspath(file)

    # Create a tree object from the XML file
    tree = ET.parse(xml_file_path)
    root = tree.getroot()
    namespaces = {"ex": "http://www.ecb.int/vocabulary/2002-08-01/eurofxref"}
    exchange_rates = {}

    # Loop on all the "cube" nodes and fill the dictionary
    for cube in root.findall(".//ex:Cube[@time]", namespaces=namespaces):
        for child in cube.iter():
            if child.get('currency') == currency:
                exchange_rates[cube.get('time')] = float(child.get('rate'))

    return exchange_rates


def find_day_with_most_revenue(result: list) -> tuple:
    ''' The datetime in the database contains dates with exact time.
        Because we are looking for the day with most revenue we need to
        delete the time information from the query result'''

    # Temporary dictionary to process the data
    # Key = day, value = total revenue of the day
    tmp_dict = {}

    for date_time, tot_revenue in result:
        # Extract the day from the datetime
        day = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        # Accumulate the revenue of the same in the dictionary
        if day not in tmp_dict:
            tmp_dict[day] = tot_revenue
        else:
            tmp_dict[day] += tot_revenue
    # Find the key with the maximum value in the dictionary
    # which corresponds to the day with the most revenue.
    day_with_most_revenue = max(tmp_dict, key=tmp_dict.get)
    return (day_with_most_revenue, tmp_dict[day_with_most_revenue])
