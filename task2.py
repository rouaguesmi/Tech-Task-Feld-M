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

    # execute the sql query and return the result
    result = utils.connect_and_execute_query(task2_query)

    # The result of this query contains the datetimes (day+time) with the
    # revenues in a descending order.
    # However, the task is about to find out the day with the most revenue,
    # By looking at data in the Transactions table, the time in the datetime
    # is always the same (00:00:00). For this reason, the group by datetime
    # will give us the correct result.
    # Hence, the code below is doing the job only for this particular case.

    print("----------------------------------------------------------")
    print("Supposing that the transactions happened in the same times: ")
    if len(result) > 1 and len(result[0]) > 1:
        date_time, total_revenue = result[0]
        date = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")

        print(f'The day on which the most revenue was created'
              f' by mobile phone users is: {date.strftime("%Y-%m-%d")} \n'
              f'The total generated revenue is: {round(total_revenue,2)} $ ')
    else:
        print(utils.NO_RESULT_STR)

    print("----------------------------------------------------------")
    print("Supposing that the transactions happened in different times: ")

    # In order to solve this task in a more general way;
    # without supposing that the time will always be the same
    # at every transaction operation,
    # I implemented the function find_day_with_most_revenue in utils module
    # that will process the returned result and finds out
    # the day with most revenue.
    # Please refer to the function's docstring for further details.

    if len(result) > 1 and len(result[0]) > 1:
        day, total_revenue = utils.find_day_with_most_revenue(result)

        print(f'The day on which the most revenue was created'
              f' by mobile phone users is: {day} \n'
              f'The total generated revenue is: {round(total_revenue,2)} $ ')
    else:
        print(utils.NO_RESULT_STR)


if __name__ == '__main__':
    task2_solution()
