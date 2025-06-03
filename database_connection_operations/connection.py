import mysql.connector
from mysql.connector import Error

def database_connection():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='Arjit@1234',
            database='mydb'
        )

        if connection.is_connected():
            print("✅ Connected to MySQL")
            return connection

    except Error as e:
        print(f"❌ Error: {e}")
        return None
