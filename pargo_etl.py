import pandas as pd 
import pyodbc
import os
import random

conn = pyodbc.connect('Driver={SQL Server Native Client 11.0};'                           
                    'Server=(LocalDb)\pargoinstance;'
                    'database=pargo_datalake;'
                    'Trusted_Connection=yes;')
cursor = conn.cursor()
customer = pd.read_csv('data/customer.csv')
parcels= pd.read_csv('data/parcels.csv')
pickup_points = pd.read_csv('data/pick_up_points.csv')

print("Loading customer table...")
a = 0
for row in customer.itertuples():
    cursor.execute('''
                INSERT INTO customer (Customer_ID,Customer_Name,Customer_Cell)
                VALUES (?,?,?)
                ''',
                str(row.Customer_ID), 
                str(row.Customer_Name),
                 str(row.Customer_Cell)
                )
    a = a + 1
print("Rows inserted in pickup point table: ", a)

print("Loading pickup_points table...")
b = 0
for row in pickup_points.itertuples():
    cursor.execute('''
                INSERT INTO Pick_up_points (Pickup_Point_ID,Suburb,Province,Regional)
                VALUES (?,?,?,?)
                ''',
                str(row.Pickup_Point_ID), 
                str(row.Suburb),
                str(row.Province),
                str(row.Regional)
                )
    b = b + 1
print("Rows inserted in pickup point table: ", b)
c = 0
print("Loading Parcels table...")
for row in parcels.itertuples():
    cursor.execute('''
                INSERT INTO parcels (Waybill,Customer_ID,Order_Date,Parcel_KG,Courier_Charge,Sales_amount,Pickup_Point_ID)
                VALUES (?,?,?,?,?,?,?)
                ''',
                str(row.Waybill), 
                str(row.Customer_ID),
                str(row.Order_Date),
                str(row.Parcel_KG),
                str(row.Courier_Charge),
                str(row.Sales_amount),
                str(row.Pickup_Point_ID)
                )
    c = c + 1
print("Rows inserted in parcels table: ", c)
conn.commit()               
    