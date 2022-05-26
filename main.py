import numpy as np
import pandas as pd
import db_con as db

regions = pd.read_csv('region.csv', index_col=False)
customer_details = pd.read_csv('customers.csv', index_col=False)
car_details = pd.read_csv('car_details.csv', index_col=False)
branch_details = pd.read_csv('branch_details.csv', index_col=False)
sales_data = pd.read_csv('sales_data.csv', index_col=False)

conn = db.Database()
cursor = conn.connect(host_name='localhost', user_name='root', pass_word='password')

# Inserting in Regions table
try:
    conn = sql.connect(host='localhost', user='root', password='password')
    if conn.is_connected():
        cursor = conn.cursor()
except Error as e:
    print("Error")


# For Inserting Data from Region into tables

try:
    for i,row in regions.iterrows():
        sql = "INSERT INTO TATA_MOTORS.REGION VALUES (%s)"
        cursor.execute(sql, tuple(row))
        print('record inserted!')
        conn.commit()
except Error as e:
    print('Trouble inserting the data')
    print(e)

# inserting data from Cars into car_details table
try:
    for i, row in cars.iterrows():
        query = "INSERT INTO TATA_MOTORS.CAR_DETAILS VALUES (%s,%s,%s,%s)"
        cursor.execute(query, tuple(row))
        print('record inserted!')
        conn.commit()
except Error as e:
    print('Trouble inserting the data')
    print(e)


# inserting data from branches to branch_details
try:
    for i,row in branches.iterrows():
        query = "INSERT INTO TATA_MOTORS.BRANCH_DETAILS VALUES (%s,%s,%s,%s)"
        cursor.execute(query, tuple(row))
        print('record inserted!')
        conn.commit()
except Error as e:
    print('Trouble inserting the data')
    print(e)

# inserting into customer_details table
try:
    for i,row in customers.iterrows():
        query = "INSERT INTO TATA_MOTORS.CUSTOMER_DETAILS VALUES (%s,%s,%s,%s,%s,%s)"
        cursor.execute(query, tuple(row))
        print('record inserted!')
        conn.commit()
except Error as e:
    print('Trouble inserting the data')
    print(e)
# changing the sale date to correct format
sales_data['SALE_DATE'] = sales_data['SALE_DATE'].astype(str)

# inserting sales data into sales_data table
try:
    for i,row in sales_data.iterrows():
        query = "INSERT INTO TATA_MOTORS.SALES_DATA (SALE_ID, BRANCH_ID, CUSTOMER_ID, CAR_MODEL, SALE_DATE, NUM_PLATE) VALUES (%s,%s,%s,%s,%s,%s)"
        cursor.execute(query, (None,row[1],row[2],row[3],row[4],row[5]))
        print('record inserted!')
        conn.commit()
except Error as e:
    print('Trouble inserting the data')
    print(e)
