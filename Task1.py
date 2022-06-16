import sqlite3

def task1() :
    """ finds out the visitor who created the most revenue in the database transactions.db """
    connection = sqlite3.connect('transactions.db')
    cursor = connection.cursor()

    task1_request = ''' SELECT visitor_id , SUM(revenue) AS visitor_revenue 
                       FROM Transactions 
                       GROUP BY visitor_id
                       ORDER BY visitor_revenue DESC
                       LIMIT 1 ''' 
    cursor.execute(task1_request)
    print("The visitor's id having the highest revenue is : " , cursor.fetchall()[0][0])
    connection.close()

if __name__ == '__main__' : 
    task1() 
