''' Solution for task 1 '''
import commun


def task1_solution():
    """ finds out the visitor who created the most revenue
         in the database transactions.db """

    # sql query for task 1
    task1_query = '''SELECT visitor_id , SUM(revenue) AS visitor_revenue
    FROM transactions GROUP BY visitor_id
    ORDER BY visitor_revenue DESC LIMIT 1'''

    # execute the sql query and print the result
    result = commun.connect_and_execute_query(task1_query)

    if len(result) > 1 and len(result[0]) > 1:
        print("The visitor's id having the highest revenue is : ", result[0][0])
    else:
        print("No result was found in the database")


if __name__ == '__main__':
    task1_solution()
