import mysql.connector
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

try:
    conn = mysql.connector.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASSWORD,
    )
    print("Connected successfully!")

    cursor = conn.cursor()
    cursor.execute("SHOW DATABASES;")
    for db in cursor.fetchall():
        print(db)

    cursor.close()
    conn.close()

except mysql.connector.Error as err:
    print(f"Error: {err}")