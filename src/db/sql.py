import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

class SQL:
    def __init__(self):
        try:
            self.connection = mysql.connector.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASSWORD,
                database=DB_NAME
            )
            self.cursor = self.connection.cursor()
        except mysql.connector.Error as err:
            print(f"Error: {err}")

    def close(self):
        self.cursor.close()
        self.connection.close()

    def read_data(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()

    def execute_many(self, query, values):
        self.cursor.executemany(query, values)
        self.connection.commit()