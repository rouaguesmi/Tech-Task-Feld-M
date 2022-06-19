'''Solution to task 4'''

from datetime import datetime, timedelta
from os import path
import sqlite3
from typing import final

import utils 


def task4_solution():
    ''' write me'''

    update_list = []

    currency_rates = utils.extract_exchange_rates()

    sql_query = ''' SELECT id, datetime FROM Transactions '''

    result = utils.connect_and_execute_query(sql_query)

    for transaction_id, date_time in result:
        date_obj = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")

        # If a date from the transactions table is a week-end day, the XML
        # currency exchange file does not contain the exchange rate for
        # that day, so we use the rate of the last working day of
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
            with connection.cursor() as cursor:
                update_query = '''  UPDATE Transactions
                                    SET revenue = revenue / ? WHERE id = ?'''
                cursor.executemany(update_query, update_list)

                connection.commit()
    except Exception as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()


task4_solution()
