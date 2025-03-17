import json
from datetime import datetime

from sklearn.metrics.pairwise import cosine_similarity

from src.db.sql import SQL

sql = SQL()
SPACE_ID = "lido-snapshot.eth"
VOTER_COUNT_LIMIT = 17

print("Fetching proposals...")
data = sql.read_data(f"SELECT id FROM proposals where space_id = '{SPACE_ID}'")

proposals_mapping = {proposal_id: index for index, proposal_id in enumerate(map(lambda item: item[0], data))}

print("[{}] Number of proposals: {}".format(datetime.now().strftime("%H:%M:%S"), len(proposals_mapping.keys())))

print("Fetching top voters...")
TOP_VOTER_QUERY = f"SELECT count(*) as count, voter FROM votes where space_id = '{SPACE_ID}' group by voter order by count(*) desc limit {VOTER_COUNT_LIMIT}"

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

    print(f"[{datetime.now().strftime("%H:%M:%S")}] Fetched {len(data)} votes from {voter}")

    for item in data:
        proposal_id, voter, choice = item
        unique_int = hash(json.dumps(choice, sort_keys=True))

        voting_map[voter][proposals_mapping[proposal_id]] = unique_int

print("Finished fetching all votes.")

with open(f"{SPACE_ID}_voting_map.csv", "w") as f:
    for voter in top_voters:
        f.write(f"{",".join(map(lambda x: str(x), voting_map[voter]))}\n")

coalitions = [[1 for i in range(len(top_voters))] for i in range(len(top_voters))]
count = 0

total = len(top_voters) * (len(top_voters) - 1) // 2

start_time = datetime.now()
for i in range(0, len(top_voters)):
    for j in range(i + 1, len(top_voters)):
        coalitions[i][j] = coalitions[j][i] = float(cosine_similarity([voting_map[top_voters[i]]], [voting_map[top_voters[j]]])[0][0])
        count += 1
        elapsed_time = datetime.now() - start_time
        avg_time_per_iteration = elapsed_time / count
        remaining_iterations = total - count
        estimated_completion_time = datetime.now() + avg_time_per_iteration * remaining_iterations
        print(
            f"[{datetime.now()}] Calculated {i}-{j};Estimated completion time: {estimated_completion_time.strftime('%H:%M:%S')}")

with open(f"{SPACE_ID}_coalitions.csv", "w") as f:
    for item in coalitions:
        f.write(f"{",".join(map(lambda x: str(x), item))}\n")

with open(f"{SPACE_ID}_top_voters.txt", "w") as f:
    for voter in top_voters:
        f.write(f"{voter}\n")