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
        database=os.getenv("DB_NAME")
    )
    print("Connected successfully!")

    cursor = conn.cursor()
    # cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_NAME};")
    # print(f"Database '{DB_NAME}' created successfully!")

    table_name = "spaces"
    cursor.execute(f"DESCRIBE {table_name};")
    columns = cursor.fetchall()

    # Print summary
    print(f"Structure of table `{table_name}`:")
    for column in columns:
        print(
            f"Column: {column[0]}, Type: {column[1]}, Nullable: {column[2]}, Key: {column[3]}, Default: {column[4]}, Extra: {column[5]}")

    # Close connection
    cursor.close()
    conn.close()

except mysql.connector.Error as err:
    print(f"Error: {err}")