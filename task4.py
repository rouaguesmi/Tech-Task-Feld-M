'''Solution to task 4'''
from datetime import datetime, timedelta
from os import path
import sqlite3
import commun


def task4_solution():
    ''' write me'''
    update_list = []
    currency_rates = commun.extract_exchange_rates()
    sql_query = ''' SELECT id, datetime FROM Transactions '''
    result = commun.connect_and_execute_query(sql_query)
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
    dbfile_path = path.abspath(commun.DB_NAME)
    # Update the table :
    connection = sqlite3.connect(dbfile_path)
    cursor = connection.cursor()
    update_query = '''  UPDATE Transactions
                        SET revenue = revenue / ? WHERE id = ?'''
    cursor.executemany(update_query, update_list)
    connection.commit()
    connection.close()


task4_solution()
