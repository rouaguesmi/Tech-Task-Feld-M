
import sqlite3
import csv
import os.path as path

FILE_NAME = "Transactions.csv"

def Task3():
    connection = sqlite3.connect('transactions.db')
    cursor = connection.cursor()
    

    task3Request = '''SELECT Transactions.* , devices.*
                      FROM Transactions INNER JOIN devices ON devices.id = Transactions.device_type
                      ''' 
                  
    cursor.execute(task3Request)
    result = cursor.fetchall()
    
    #Get absolute path for the file 
    file_path = path.abspath(FILE_NAME)
    with open(file_path, 'w') as f:
        writer = csv.writer(f)
        for row in result: 
            writer.writerow(row)
    
   
    connection.close()

if __name__ == '__main__':
    Task3()
    

