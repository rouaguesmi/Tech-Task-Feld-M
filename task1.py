'''Solution for task 1'''

import utils


def task1_solution():
    """ finds out the visitor who generated the most revenue
         in the sqlite database transactions.db and print
         its ID. """

    # sql query for task 1
    task1_query = '''SELECT visitor_id , SUM(revenue) AS visitor_revenue
                        FROM transactions GROUP BY visitor_id
                        ORDER BY visitor_revenue DESC'''

    # execute the sql query and print the result
    result = utils.connect_and_execute_query(task1_query)

    if len(result) > 1 and len(result[0]) > 1:
        print(f"The visitor's id who created the most"
              f" revenue is: {result[0][0]}\n"
              f"The revenue created is: {round(result[0][1],2)} $")
    else:
        print(utils.NO_RESULT_STR)


if __name__ == '__main__':
    task1_solution()
