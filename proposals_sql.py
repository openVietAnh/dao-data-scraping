import csv
import requests
import mysql.connector
import json
import re
import os
from dotenv import load_dotenv

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
)
cursor = conn.cursor()

# Function to remove emojis
def remove_emojis(text):
    if isinstance(text, str):  # Ensure it's a string before removing emojis
        return re.sub(r'[\U00010000-\U0010FFFF]', '', text, flags=re.UNICODE)
    return text

# Read space IDs from CSV file
spaces_id = []
with open('spaces_by_proposals_count.csv', 'r', encoding='utf8') as f:
    reader = csv.reader(f, delimiter=',')
    next(reader, None)  # Skip header

    for item in reader:
        spaces_id.append(item[0])

# Process each space ID
for space_id in spaces_id[:50]:
    url = "https://hub.snapshot.org/graphql?"
    headers = {"accept": "application/json", "content-type": "application/json"}

    start_time = 1605388716
    all_proposals = []  # Store proposals for bulk insertion

    while True:
        query = f"""
        query Proposals {{
          proposals (
            first: 1000,
            skip: 0,
            where: {{
              created_gte: {start_time},
              space_in: ["{space_id}"]
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

        # Prepare and send request
        payload = {"query": query, "variables": None, "operationName": "Proposals"}
        response = requests.post(url, headers=headers, json=payload)

        if response.status_code == 200:
            data = response.json()
            fetched_proposals = data.get("data", {}).get("proposals", [])

            if not fetched_proposals:
                break  # No new proposals, stop fetching

            print(f"Fetched {len(fetched_proposals)} proposals from {start_time}")

            # Prepare bulk insert data
            for item in fetched_proposals:
                proposal_id = remove_emojis(item["id"])  # Remove emojis from ID
                title = remove_emojis(item.get("title", ""))
                body = remove_emojis(item.get("body", ""))  # ✅ Remove emojis from body
                choices = json.dumps([remove_emojis(choice) for choice in item.get("choices", [])])  # ✅ Remove emojis from choices
                start = item["start"]
                end = item["end"]
                snapshot = item["snapshot"]
                state = item["state"]
                author = item["author"]

                all_proposals.append(
                    (proposal_id, space_id, title, body, choices, start, end, snapshot, state, author)
                )

            start_time = fetched_proposals[-1]["start"]  # Update last timestamp

            if len(fetched_proposals) < 1000:
                break

        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
            break  # Stop on failure

    # Bulk insert proposals into MySQL
    if all_proposals:
        sql = """
        INSERT INTO proposals (id, space_id, title, body, choices, start, end, snapshot, state, author)
        VALUES (%s, %s, %s, %s, %s, FROM_UNIXTIME(%s), FROM_UNIXTIME(%s), %s, %s, %s)
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
        cursor.executemany(sql, all_proposals)  # Bulk insert
        conn.commit()

# Close MySQL connection
cursor.close()
conn.close()
print("Proposals successfully saved to MySQL in bulk!")
