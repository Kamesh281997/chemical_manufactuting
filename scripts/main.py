import psycopg2
from config import config
import pandas as pd
import os
from sqlalchemy import create_engine

direct_path="C:/Users/kames/OneDrive/Desktop/capstone_main/data/Hourly_Standard/Cleaned_Process"

def connect(extrt):
    connection = None
    try:
        params = config()
        print('Connecting to the PostgreSQL database...')
        connection = psycopg2.connect(**params)
        crsr = connection.cursor()
        print('PostgreSQL database version:')
        crsr.execute('SELECT version()')
        db_version = crsr.fetchone()
        print(db_version)
        if extrt=="insert":
            create_dataframe(crsr, 'process')
        if extrt=="extract": 
            return extract_data_from_table(crsr, "process")

        crsr.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if connection is not None:
            connection.close()
            print('Database connection terminated.')

def create_dataframe(cursor,table_name):
    if not os.path.exists(direct_path):
        print("Directory path is wrong!!!")
    else:
        
        for filename in os.listdir(direct_path):
            filename=os.path.join(direct_path,filename)
            if filename.endswith(".csv"):
                df=pd.read_csv(filename)
                df.drop("Unnamed: 0",axis=1,inplace=True)
                insert_dataframe_into_table(df,cursor,table_name)
    

def extract_data_from_table(cursor, table_name):

    try:
        cursor.execute(f'SELECT * FROM {table_name}')
        rows = cursor.fetchall()
        print(f'Data from table {table_name}:')
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        print(df)
        return df
    except psycopg2.Error as e:
        print(f'Error extracting data from {table_name}: {e}')
        
    #     try:
    #     cursor.execute(f'SELECT * FROM {table_name}')
    #     rows = cursor.fetchall()
    #     columns = [desc[0] for desc in cursor.description]
    #     df = pd.DataFrame(rows, columns=columns)
    #     return df
    # except psycopg2.Error as e:
    #     print(f'Error extracting data from {table_name}: {e}')
    #     return pd.DataFrame() 
        
# def insert_data_into_table(cursor, table_name):
  
#     try:
#         cursor.execute(f"INSERT INTO {table_name} (column1, column2, column3) VALUES (%s, %s, %s)",
#                        ('value1', 'value2', 'value3'))

#         print('Data inserted successfully.')
#     except psycopg2.Error as e:
#         print(f'Error inserting data into {table_name}: {e}')
        
def insert_dataframe_into_table(dataframe, cursor, table_name):
    try:
        columns = ', '.join(dataframe.columns)
        values = ', '.join('%s' for _ in dataframe.columns)
        
        for row in dataframe.itertuples(index=False, name=None):
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(['%s' for _ in row])})"
            cursor.execute(insert_query, row)
        cursor.connection.commit()

        print(f'DataFrame inserted into {table_name} successfully using cursor.')
    except Exception as e:
        print(f'Error inserting DataFrame into {table_name}: {e}')


