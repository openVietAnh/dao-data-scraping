import csv
import requests
import mysql.connector
from dotenv import load_dotenv
import os
import json
import re

# Load environment variables
load_dotenv()

# Database credentials from .env
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Connect to MySQL database with utf8mb4 support
conn = mysql.connector.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
    charset="utf8mb4",
    collation="utf8mb4_unicode_ci",
)
cursor = conn.cursor()

# Function to remove emojis and non-ASCII characters
def remove_emojis(text):
    return re.sub(r'[^\x00-\x7F]+', '', text) if text else text

# Read spaces from CSV
spaces_id = []
with open('spaces_by_proposals_count.csv', 'r', encoding='utf8') as f:
    reader = csv.reader(f, delimiter=',')
    next(reader, None)  # Skip header
    for item in reader:
        spaces_id.append(item[0])

# Define GraphQL API endpoint and headers
url = "https://hub.snapshot.org/graphql?"
headers = {
    "accept": "application/json",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "content-type": "application/json",
}

# Loop through the first 50 spaces
for space_id in spaces_id[:50]:
    start_time = 1605388716

    while True:
        query = f"""
        query Proposals {{
        proposals (
            first: 1000,
            skip: 0,
            where: {{
                created_gte: {start_time}
                space_in: "{space_id}"
            }}
            orderBy: "created",
            orderDirection: asc
        ) {{
            id
            title
            body
            choices
            start
            end
            snapshot
            state
            author
        }}
        }}
        """

        # Send request to API
        response = requests.post(url, headers=headers, json={"query": query, "variables": None, "operationName": "Proposals"})

        if response.status_code == 200:
            data = response.json()
            fetched_proposals = data.get("data", {}).get("proposals", [])

            if not fetched_proposals:
                break  # No more proposals, stop fetching

            print(f"Fetched {len(fetched_proposals)} proposals from {start_time}")

            # Prepare bulk insert data
            values = [
                (
                    proposal["id"],
                    remove_emojis(proposal.get("title", "")),  # Filter emojis
                    remove_emojis(proposal.get("body", "")),   # Filter emojis
                    json.dumps(proposal.get("choices", [])),   # Convert choices to JSON
                    proposal["start"],
                    proposal["end"],
                    proposal["snapshot"],
                    proposal["state"],
                    proposal["author"],
                    space_id  # Store the space_id for reference
                )
                for proposal in fetched_proposals
            ]

            # SQL Query for Bulk Insert with ON DUPLICATE KEY UPDATE
            sql = """
            INSERT INTO proposals (id, title, body, choices, start, end, snapshot, state, author, space_id)
            VALUES (%s, %s, %s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s), %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE 
                title = VALUES(title), 
                body = VALUES(body), 
                choices = VALUES(choices), 
                start = VALUES(start), 
                end = VALUES(end), 
                snapshot = VALUES(snapshot), 
                state = VALUES(state), 
                author = VALUES(author);
            """

            # Execute batch insert
            cursor.executemany(sql, values)
            conn.commit()  # Commit changes

            # Update last timestamp
            start_time = fetched_proposals[-1]["start"]

        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
            break  # Stop on request failure

# Close database connection
cursor.close()
conn.close()

print("Data successfully saved to MySQL in bulk!")
