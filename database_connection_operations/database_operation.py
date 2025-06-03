
from .connection import database_connection
from database_connection_operations.read_file import read_csv_in_chunks

file_path = 'customers.csv' 

def insert_data_into_table():
    conn = database_connection()
    if conn is None:
        print(" Failed to connect to the database. Exiting.")
        return
    cursor = conn.cursor()

    table_name = 'customers'
    columns = ['Index','Customer Id','First Name','Last Name','Company','City','Country','Phone 1','Phone 2','Email','Subscription Date','Website']

    placeholders = ', '.join(['%s'] * len(columns))
    insert_query = f"INSERT INTO {table_name} ({', '.join(f'`{col}`' for col in columns)}) VALUES ({placeholders})"


    for chunk in read_csv_in_chunks(file_path, chunk_size=100):
        print(f"Inserting chunk of {len(chunk)} rows")

        values = [tuple(row[col] for col in columns) for row in chunk]

        try:
            cursor.executemany(insert_query, values)
            conn.commit()
            print("Chunk inserted successfully")
        except Exception as e:
            print(f"Error inserting chunk: {e}")
            conn.rollback()

    cursor.close()
    conn.close()
    print("Database connection closed")

insert_data_into_table()
