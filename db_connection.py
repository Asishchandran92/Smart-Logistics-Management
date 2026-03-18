import mysql.connector
from mysql.connector import Error

def get_connection():
    try:
        connection = mysql.connector.connect(
          host="localhost",
          user="root",
          password="root123",
          database="smart_logistics"
        )

        return connection
    except Error as e:
        print("Database connection error:", e)
        return None
