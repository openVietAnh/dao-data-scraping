import json

from src.db.sql import SQL
from src.graphql.snapshot_request import request
from src.utils.emoji_utils import remove_emojis


def get_snapshot_proposals(space_id):
    db_connection = SQL()
    start_time, proposals = 0, set()

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

        response = request(query, "Proposals")

        if response.status_code == 200:
            data = response.json()
            fetched_proposals = data.get("data", {}).get("proposals", [])
            ind = 0
            while ind < len(fetched_proposals) and fetched_proposals[ind].get("id", "") in proposals:
                ind += 1
            if ind == len(fetched_proposals):
                break
            proposals.add(map(lambda proposal: proposal["id"], fetched_proposals[ind:]))
            print(f"Fetched {len(fetched_proposals) - ind} proposals from {start_time}")
            values = [(
                item["id"],
                space_id,
                item.get("title", ""),
                remove_emojis(item.get("body", "")),
                json.dumps(
                    [choice for choice in item.get("choices", [])]
                ),
                item["start"],
                item["end"],
                item["snapshot"],
                item["state"],
                item["author"]
            ) for item in fetched_proposals[ind:]]
            db_connection.execute_many("""
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
                """, values)
            if len(fetched_proposals) < 1000:
                break
            start_time = fetched_proposals[-1].get("created", start_time)
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
            break

    db_connection.close()
