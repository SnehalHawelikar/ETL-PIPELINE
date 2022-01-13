import sqlite3
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore")

class Pipeline(object):
    def __init__(self):
        self.superstore = None

    def extract(self):
        """
        Data source (based on data from data.world)
        description:
            data = https://data.world/2918diy/global-superstore
        """
        url_superstore = 'F:\Snehal\Masters_Study\Study-SEM3\DataManagement2\Exam\Global_Superstore.csv'

        self.superstore = pd.read_csv(url_superstore, encoding='ISO-8859-1')

    def transform(self):

        self.superstore['Order_Date'] = pd.to_datetime(self.superstore['Order_Date'])
        self.superstore['Ship_Date'] = pd.to_datetime(self.superstore['Ship_Date'])  

        self.superstore['Profit'] = self.superstore['Profit'].astype(str)
        self.superstore['Profit'] = self.superstore['Profit'].str.replace(",","")
        self.superstore['Profit'] = self.superstore['Profit'].str.replace("$","")
        self.superstore['Profit'] = self.superstore['Profit'].astype(float)

        self.superstore['Sales'] = self.superstore['Sales'].astype(str)
        self.superstore['Sales'] = self.superstore['Sales'].str.replace(",","")
        self.superstore['Sales'] = self.superstore['Sales'].str.replace("$","")
        self.superstore['Sales'] = self.superstore['Sales'].astype(float)
    

    def load(self):
        db = DB()
        #self.superstore.to_sql('Groceries', db.conn, if_exists='append', index=False)


class DB(object):

    def __init__(self, db_file='db.sqlite'):
        self.conn = sqlite3.connect(db_file)
        self.cur = self.conn.cursor()
        self.__init_db()

    def __del__(self):
        self.conn.commit()
        self.conn.close()

    def __init_db(self):
        table1 = f"""CREATE TABLE IF NOT EXISTS MasterData(
            Order_ID PRIMARY KEY,
            Customer_ID,
            City,
            State,
            Country, 
            Region,
            Product_ID,
            Discount,
            Sales,
            Profit,
            Shipping_Cost 
            );"""

        table2 = f"""CREATE TABLE IF NOT EXISTS Orders(
            Order_ID PRIMARY KEY,
            Order_Date,
            Ship_Date,
            Ship_Mode,
            Market,
            Order_Priority,
            FOREIGN KEY (Order_ID) REFERENCES MasterData(Order_ID)
            );"""
        
        table3 = f"""CREATE TABLE IF NOT EXISTS Customers(
            Customer_ID PRIMARY KEY,
            Customer_Name,
            FOREIGN KEY (Customer_ID) REFERENCES MasterData(Customer_ID)
            );"""

        table4 = f"""CREATE TABLE IF NOT EXISTS Products(
            Product_ID PRIMARY KEY,
            Product_Name,
            Category,
            Sub_Category,
            Quantity,
            FOREIGN KEY (Product_ID) REFERENCES MasterData(Product_ID)
            );"""

        self.cur.execute(table1)
        self.cur.execute(table2)
        self.cur.execute(table3)
        self.cur.execute(table4)


if __name__ == '__main__':
    pipeline = Pipeline()
    print('Data Pipeline created')
    print('\t extracting data from source .... ')
    pipeline.extract()
    print('\t formatting and transforming data ... ')
    pipeline.transform()
    print('\t loading into database ... ')
    pipeline.load()

    print('\nDone. See: result in "db.sqlite"')


# Create an external Star schema 

input = 'Global_Superstore.csv'

preprocessed_file = 'Global_Superstore_Preprocessed.csv'

file_orders_output = 'Order_Details.xlsx'
file_customers_output = 'Customer_Details.xlsx'
file_products_output = 'Product_Details.xlsx'
file_fact_table_output = 'Fact_Table.xlsx'

df = pd.read_csv(input, header=0, encoding='ISO-8859-1')

#Data preprocessing 

df['Order_Date'] = pd.to_datetime(df['Order_Date'])
df['Ship_Date'] = pd.to_datetime(df['Ship_Date'])  

df['Profit'] = df['Profit'].astype(str)
df['Profit'] = df['Profit'].str.replace(",","")
df['Profit'] = df['Profit'].str.replace("$","")
df['Profit'] = df['Profit'].astype(float)

df['Sales'] = df['Sales'].astype(str)
df['Sales'] = df['Sales'].str.replace(",","")
df['Sales'] = df['Sales'].str.replace("$","")
df['Sales'] = df['Sales'].astype(float)

df.to_csv(preprocessed_file, encoding='ISO-8859-1', index=False)

df_pre_processed = pd.read_csv(preprocessed_file, header = 0, encoding='ISO-8859-1')

# Select the columns as per requirement from df_pre_processed dataframe into different dataframes.

df_orders = df_pre_processed.loc[ : , ['Order_ID', 'Order_Date', 'Ship_Date', 'Ship_Mode', 'Segment', 'Market','Order_Priority']]

df_customers = df_pre_processed.loc[ : , ['Customer_ID', 'Customer_Name'] ]

df_products = df_pre_processed.loc[ : , ['Product_ID', 'Product_Name', 'Category', 'Sub_Category', 'Quantity']]

df_fact = df_pre_processed.loc[ : , ['Order_ID', 'Customer_ID', 'City', 'State', 'Country', 'Region', 'Product_ID', 'Discount', 'Sales', 'Profit', 'Shipping_Cost'] ]

# Sort the dataframe in ascending order based on Customer_ID and Product_ID
# remove duplicate Customer_ID records
df_customers = df_customers.sort_values(by = ['Customer_ID'], ascending = True, na_position = 'last').drop_duplicates(['Customer_ID'],keep = 'first')
# remove duplicate Product_ID records
df_products = df_products.sort_values(by = ['Product_ID'], ascending = True, na_position = 'last').drop_duplicates(['Product_ID'],keep = 'first')

#Export the star schema and save them in Excel file
excel_writer_orders = pd.ExcelWriter(file_orders_output)
df_orders.to_excel(excel_writer_orders,'Orders', index = False)
excel_writer_orders.save()

excel_writer_customers = pd.ExcelWriter(file_customers_output)
df_customers.to_excel(excel_writer_customers,'Customers', index = False)
excel_writer_customers.save()

excel_writer_products = pd.ExcelWriter(file_products_output)
df_products.to_excel(excel_writer_products,'Products', index = False)
excel_writer_products.save()

excel_writer_fact = pd.ExcelWriter(file_fact_table_output)
df_fact.to_excel(excel_writer_fact,'Facts', index = False)
excel_writer_fact.save()

print('\nDone. See: Results in the directory!')




