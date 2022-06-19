'''Solution for task 2'''

from datetime import datetime
import utils


def process_result(result: list) -> dict:
    ''' process result to delete time from date '''
    # [(2018-10-5 00:12:15, 5041) .... ]
    new_dict = {}
    for date_time, tot_revenue in result:
        date = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
        if date not in new_dict:
            new_dict[date] = 0
        new_dict[date] += tot_revenue

    max_value = max(new_dict, key=new_dict.get)
    return max_value, new_dict[max_value]


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
    
    _date, tot_revenue = process_result(result)


    print(f'The day on which the most revenue was created'
            f' by mobile phone users is: {_date} \n'
            f'The total generated revenue is: {round(tot_revenue,2)} $ ')

    if len(result) > 1 and len(result[0]) > 1:
        date_time, total_revenue = result[0]
        date = datetime.strptime(date_time, "%Y-%m-%d %H:%M:%S")

        print(f'The day on which the most revenue was created'
                f' by mobile phone users is: {date.strftime("%Y-%m-%d")} \n'
                f'The total generated revenue is: {round(total_revenue,2)} $ ')
    else:
        print(utils.NO_RESULT_STR)


if __name__ == '__main__':
    task2_solution()
