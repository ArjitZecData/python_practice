import mysql.connector
from mysql.connector import Error

try:
    connection = mysql.connector.connect(
        host='localhost',       
        user='root',           
        password='Arjit@1234',
        database='mydb' 
    )

    if connection.is_connected():
        print("✅ Connected to MySQL")

except Error as e:
    print(f" Error: {e}")

finally:
    if 'connection' in locals() and connection.is_connected():
        connection.close()
        print(" MySQL connection closed")
