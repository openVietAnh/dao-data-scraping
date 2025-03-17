import json

from sklearn.metrics.pairwise import cosine_similarity

from src.db.sql import SQL

sql = SQL()
SPACE_ID = "arbitrumfoundation.eth"

print("Fetching proposals...")
data = sql.read_data(f"SELECT id FROM proposals where space_id = '{SPACE_ID}'")

proposals_mapping = {proposal_id: index for index, proposal_id in enumerate(map(lambda item: item[0], data))}

print("Number of proposals: {}".format(len(proposals_mapping.keys())))

print("Fetching top voters...")
TOP_VOTER_QUERY = f"SELECT count(*) as count, voter FROM votes where space_id = '{SPACE_ID}' group by voter order by count(*) desc limit 1165"

top_voters = [row[1] for row in sql.read_data(TOP_VOTER_QUERY)]

voting_map = {voter: [0 for item in range(len(proposals_mapping.keys()))] for voter in top_voters}

top_voters_condition = "(" + ', '.join(map(lambda voter: f"'{voter}'", top_voters)) + ")"

print("Fetching votes in batches...")

BATCH_SIZE = 1000
offset = 0

for voter in top_voters:
    VOTES_QUERY = f"""
        SELECT proposal_id, voter, choice
        FROM votes
        WHERE space_id = '{SPACE_ID}'
        AND voter = '{voter}'
    """

    data = sql.read_data(VOTES_QUERY)

    print(f"Fetched {len(data)} votes from {voter}")

    for item in data:
        proposal_id, voter, choice = item
        unique_int = hash(json.dumps(choice, sort_keys=True))  # Ensures consistent hashing

        voting_map[voter][proposals_mapping[proposal_id]] = unique_int

print("Finished fetching all votes.")

with open(f"{SPACE_ID}_voting_map.csv", "w") as f:
    for voter in top_voters:
        f.write(f"{",".join(map(lambda x: str(x), voting_map[voter]))}\n")

coalitions = [[1 for i in range(len(top_voters))] for i in range(len(top_voters))]

for i in range(0, len(top_voters)):
    for j in range(i + 1, len(top_voters)):
        coalitions[i][j] = coalitions[j][i] = float(cosine_similarity([voting_map[top_voters[i]]], [voting_map[top_voters[j]]])[0][0])

with open(f"{SPACE_ID}_coalitions.csv", "w") as f:
    for item in coalitions:
        f.write(f"{",".join(map(lambda x: str(x), item))}\n")

with open(f"{SPACE_ID}_top_voters.txt", "w") as f:
    for voter in top_voters:
        f.write(f"{voter}\n")