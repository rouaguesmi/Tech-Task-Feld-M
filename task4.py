'''Solution to task 4'''

from datetime import datetime, timedelta
from os import path
import sqlite3
import utils


def task4_solution():
    ''' Update the data stored in the database to have the created revenue
        in EUR instead of USD.'''

    update_list = []

    # Extract the exchange rates from the XML file.
    currency_rates = utils.extract_exchange_rates(utils.EUROFXREF_XML, 'USD')

    sql_query = ''' SELECT id, datetime FROM Transactions '''

    result = utils.connect_and_execute_query(sql_query)

    for transaction_id, date_time in result:
        date_obj = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")

        # If a date from the transactions table is a week-end day, the XML
        # file does not contain the corresponding exchange rate
        # so we use the rate of the last working day of
        # that week (Friday) instead.
        if date_obj.weekday() == 5:
            date_obj -= timedelta(days=1)
        elif date_obj.weekday() == 6:
            date_obj -= timedelta(days=1)

        date_str = date_obj.strftime("%Y-%m-%d")

        if date_str not in currency_rates:
            continue

        update_list.append((currency_rates[date_str], transaction_id))

    dbfile_path = path.abspath(utils.DB_NAME)

    connection = None
    try:
        # Update the transactions table with the revenue in EUR : EUR = USD / rate :
        with sqlite3.connect(dbfile_path) as connection:
            cursor = connection.cursor()
            update_query = '''  UPDATE Transactions
                                    SET revenue = revenue / ? WHERE id = ?'''
            cursor.executemany(update_query, update_list)
            connection.commit()
    except sqlite3.Error as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()

if __name__ == '__main__':
    task4_solution()
