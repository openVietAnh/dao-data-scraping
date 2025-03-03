import csv

import requests
import mysql.connector
import json
import os
import re
from datetime import datetime, timezone
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Database credentials
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Connect to MySQL database
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
    if isinstance(text, str):
        return re.sub(r'[\U00010000-\U0010FFFF]', '', text, flags=re.UNICODE)
    return text

# Space ID to fetch votes for
SPACE_ID = "uniswapgovernance.eth"

# API settings
url = "https://hub.snapshot.org/graphql?"
headers = {"accept": "application/json", "content-type": "application/json"}

batch_size = 1000  # Insert in batches of 1000
missing_proposal_ids = set()  # Track missing proposals

# Fetch existing proposal IDs from the database
cursor.execute("SELECT id FROM proposals")
existing_proposal_ids = {row[0] for row in cursor.fetchall()}

# SQL insert query for votes
sql = """
INSERT IGNORE INTO votes (id, ipfs, created, voter, space_id, proposal_id, choice, metadata, reason, vp, vp_by_strategy, vp_state) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
start_time = 1605388716
while True:
    query = f"""
    query Votes {{
        votes (
        first: 1000,
        skip: 0,
        where: {{
            created_gte: {start_time},
            space: "{SPACE_ID}"
        }}
        orderBy: "created",
        orderDirection: asc
        ) {{
        id
        ipfs
        created
        voter
        space {{
            id
        }}
        choice
        proposal {{
            id
        }}
        metadata
        reason
        vp
        vp_by_strategy
        vp_state
        }}
    }}
    """

    payload = {"query": query, "variables": None, "operationName": "Votes"}
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        data = response.json()
        fetched_votes = data.get("data", {}).get("votes", [])

        if not fetched_votes:
            break  # No more votes to fetch

        print(f"Fetched {len(fetched_votes)} votes from {start_time}")

        all_votes = []
        for item in fetched_votes:
            proposal_id = item["proposal"]["id"] if item.get("proposal") else None

            # Skip if proposal_id is missing or not in the proposals table
            if proposal_id and proposal_id not in existing_proposal_ids:
                missing_proposal_ids.add(proposal_id)
                continue

            all_votes.append((
                item["id"],
                item.get("ipfs"),
                datetime.fromtimestamp(item["created"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),  # ✅ Fixed timestamp
                item["voter"],
                item["space"]["id"],
                proposal_id,
                json.dumps(item.get("choice", {})),  # Store as JSON
                json.dumps(item.get("metadata", {})),  # Store as JSON
                remove_emojis(item.get("reason", "")),  # Remove emojis
                item["vp"],
                json.dumps(item.get("vp_by_strategy", [])),  # Store as JSON
                item["vp_state"]
            ))

        # Bulk insert every 1000 votes
        if all_votes:
            cursor.executemany(sql, all_votes)
            conn.commit()
            print(f"Inserted {len(all_votes)} votes into database.")

        # Update start_time for next request
        start_time = fetched_votes[-1]["created"]

        # If less than 1000 votes were fetched, no more to fetch
        if len(fetched_votes) < 1000:
            break

    else:
        print(f"Request failed with status code {response.status_code}: {response.text}")
        break

# Log missing proposal IDs
if missing_proposal_ids:
    print(f"Missing proposal IDs (not found in database): {missing_proposal_ids}")

cursor.close()
conn.close()
print("Votes successfully saved to MySQL!")
