import mysql.connector

def get_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="root123",
            database="smart_logistics"
        )
        return connection
