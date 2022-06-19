'''Solution for task 2'''

from datetime import datetime
import utils


def task2_solution():
    """ Find out on which day the most revenue for users
        who ordered via a mobile phone was created """

    # sql query for task 2 :
    task2_query = '''SELECT tr.datetime , SUM(tr.revenue) as total_revenue
                     FROM Transactions tr
                     INNER JOIN devices dv ON dv.id = tr.device_type
                     WHERE dv.device_name = 'Mobile Phone'
                     GROUP BY tr.datetime
                     ORDER BY total_revenue DESC'''

    # execute the sql query and print the result
    result = utils.connect_and_execute_query(task2_query)

    if len(result) > 1 and len(result[0]) > 1:
        date_time, total_revenue = result[0]
        date = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")

        print(f'The day on which the most revenue was created by mobile phone users is: {date.strftime("%Y-%m-%d")} \n'
              f'The total generated revenue is: {round(total_revenue,2)} $ ')
    else:
        print(utils.NO_RESULT_STR)


if __name__ == '__main__':
    task2_solution()
