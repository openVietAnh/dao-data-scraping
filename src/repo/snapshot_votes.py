import json
from datetime import datetime, timezone, datetime

from src.db.sql import SQL
from src.graphql.snapshot_request import request

def get_snapshot_votes(space_id, missing_file = None):
    db_connection = SQL()

    if not missing_file:
        db_connection.cursor.execute("SELECT id FROM proposals WHERE space_id = '" + space_id + "'")
        existing_proposal_ids = {row[0] for row in db_connection.cursor.fetchall()}
    else:
        with open(missing_file) as f:
            existing_proposal_ids = {proposal_id for proposal_id in map(lambda x: x.strip(), f.readlines())}

    sql = """
    INSERT IGNORE INTO votes (id, ipfs, created, voter, space_id, proposal_id, choice, metadata, reason, vp, vp_by_strategy, vp_state) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    for proposal_id in existing_proposal_ids:
        start_time = 0
        if missing_file:
            data = db_connection.read_data("SELECT created from votes where proposal_id = '" + proposal_id + "' order by created desc limit 1")
            if len(data) > 0:
                start_time = int(data[0][0].replace(tzinfo=timezone.utc).timestamp())
        while True:
            query = f"""
            query Votes {{
                votes (
                first: 1000,
                skip: 0,
                where: {{
                    created_gte: {start_time},
                    space: "{space_id}"
                    proposal_in: "{proposal_id}"
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
            response = request(query, "Votes")

            if response.status_code == 200:
                data = response.json()
                fetched_votes = data.get("data", {}).get("votes", [])
                if not fetched_votes:
                    break

                print(f"Fetched {len(fetched_votes)} votes from {start_time}: {proposal_id}")

                all_votes = []
                for item in fetched_votes:
                    proposal_id = item["proposal"]["id"] if item.get("proposal") else None

                    all_votes.append((
                        item["id"],
                        item.get("ipfs"),
                        datetime.fromtimestamp(item["created"], tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
                        item["voter"],
                        item["space"]["id"],
                        proposal_id,
                        json.dumps(item.get("choice", {})),
                        json.dumps(item.get("metadata", {})),
                        item.get("reason", ""),
                        item["vp"],
                        json.dumps(item.get("vp_by_strategy", [])),
                        item["vp_state"]
                    ))
                if all_votes:
                    db_connection.execute_many(sql, all_votes)
                    print(f"Inserted {len(all_votes)} votes into database.")

                start_time = fetched_votes[-1]["created"]

                if len(fetched_votes) < 1000:
                    break

            else:
                print(f"Request failed with status code {response.status_code}: {response.text}")
                break

    db_connection.close()
