from src.db.sql import SQL
from src.graphql.snapshot_request import request

def update_snapshot_votes(space_id):
    db_connection = SQL()
    GET_PROPOSALS_QUERY = "SELECT count(*) as count, proposal_id FROM votes WHERE space_id = '" + space_id + "' group by proposal_id"
    proposal_votes_count = {row[1]: row[0] for row in db_connection.read_data(GET_PROPOSALS_QUERY)}

    start_time = 0
    missing_votes_proposal = set()
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
            votes
            created
          }}
        }}
        """
        response = request(query, "Proposals")

        if response.status_code == 200:
            data = response.json()
            fetched_proposals = data.get("data", {}).get("proposals", [])
            if not fetched_proposals:
                break

            print(f"Fetched {len(fetched_proposals)} proposals from {start_time}")

            for item in fetched_proposals:
                try:
                    if item["votes"] > proposal_votes_count[item["id"]]:
                        print(f"Expected {item["votes"]} - Actual: {proposal_votes_count[item["id"]]}")
                        missing_votes_proposal.add(item["id"])
                except KeyError:
                    print(f"Proposal {item["id"]} not found")
                    missing_votes_proposal.add(item["id"])

            start_time = fetched_proposals[-1]["created"]

            if len(fetched_proposals) < 1000:
                break

        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
            break

    db_connection.close()
    with open(f"{space_id}_missing_votes.txt", "w") as f:
        for item in missing_votes_proposal:
            f.write(f"{item}\n")
