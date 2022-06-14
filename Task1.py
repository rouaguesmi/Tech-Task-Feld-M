from multiprocessing import connection
import sqlite3

from colorama import Cursor


def task1() :

    connection = sqlite3.connect('transactions.db')
    cursor = connection.cursor()

    task1Request = ''' SELECT visitor_id , SUM(revenue) AS visitor_revenue 
                       FROM Transactions 
                       GROUP BY visitor_id
                       ORDER BY visitor_revenue DESC
                       LIMIT 1 ''' 
    cursor.execute(task1Request)
    print(cursor.fetchall()[0][0])
    connection.close()

if __name__ == '__main__' : 
    task1() 
    