import csv

import requests
import mysql.connector
import json
import os
import re
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", 3306))
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

conn = mysql.connector.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=DB_USER,
    password=DB_PASSWORD,
    database=DB_NAME,
)
cursor = conn.cursor()

def remove_emojis(text):
    if isinstance(text, str):
        return re.sub(r'[\U00010000-\U0010FFFF]', '', text, flags=re.UNICODE)
    return text

SPACE_ID = ""

spaces_id = []
with open('top_dao', 'r', encoding='utf8') as f:
    lines = f.readlines()

    for item in lines:
        spaces_id.append(item.strip())

print(spaces_id)

url = "https://hub.snapshot.org/graphql?"
headers = {"accept": "application/json", "content-type": "application/json"}

batch_size = 1000
missing_proposal_ids = set()

cursor.execute("SELECT id FROM proposals")
existing_proposal_ids = {row[0] for row in cursor.fetchall()}

sql = """
INSERT IGNORE INTO votes (id, ipfs, created, voter, space_id, proposal_id, choice, metadata, reason, vp, vp_by_strategy, vp_state) 
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for space_id in spaces_id:
    start_time = 1605388716
    while True:
        query = f"""
        query Votes {{
            votes (
            first: 1000,
            skip: 0,
            where: {{
                created_gte: {start_time},
                space: "{space_id}"
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
                break

            print(f"Fetched {len(fetched_votes)} votes from {start_time}: {space_id}")

            all_votes = []
            for item in fetched_votes:
                proposal_id = item["proposal"]["id"] if item.get("proposal") else None

                if proposal_id and proposal_id not in existing_proposal_ids:
                    missing_proposal_ids.add(proposal_id)
                    continue

                all_votes.append((
                    item["id"],
                    item.get("ipfs"),
                    datetime.fromtimestamp(item["created"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                    item["voter"],
                    item["space"]["id"],
                    proposal_id,
                    json.dumps(item.get("choice", {})),
                    json.dumps(item.get("metadata", {})),
                    remove_emojis(item.get("reason", "")),
                    item["vp"],
                    json.dumps(item.get("vp_by_strategy", [])),
                    item["vp_state"]
                ))

            if all_votes:
                cursor.executemany(sql, all_votes)
                conn.commit()
                print(f"Inserted {len(all_votes)} votes into database.")

            start_time = fetched_votes[-1]["created"]

            if len(fetched_votes) < 1000:
                break

        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
            break

if missing_proposal_ids:
    print(f"Missing proposal IDs (not found in database): {missing_proposal_ids}")

cursor.close()
conn.close()
print("Votes successfully saved to MySQL!")
