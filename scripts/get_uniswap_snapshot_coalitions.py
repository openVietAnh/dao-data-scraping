from sklearn.metrics.pairwise import cosine_similarity

from src.db.sql import SQL

sql = SQL()

print("Fetching proposals...")
data = sql.read_data("SELECT id FROM proposals where space_id = 'uniswapgovernance.eth'")

proposals_mapping = {proposal_id: index for index, proposal_id in enumerate(map(lambda item: item[0], data))}

print("Number of proposals: {}".format(len(proposals_mapping.keys())))

print("Fetching top voters...")
TOP_VOTER_QUERY = "SELECT count(*) as count, voter FROM votes where space_id = 'uniswapgovernance.eth' group by voter order by count(*) desc limit 15"

top_voters = [row[1] for row in sql.read_data(TOP_VOTER_QUERY)]

voting_map = {voter: [0 for item in range(len(proposals_mapping.keys()))] for voter in top_voters}

top_voters_condition = "(" + ', '.join(map(lambda voter: f"'{voter}'", top_voters)) + ")"

print("Fetching votes...")
VOTES_QUERY = "select proposal_id, voter from votes where space_id = 'uniswapgovernance.eth' and voter in " + top_voters_condition

print(VOTES_QUERY)

voting_data = sql.read_data(VOTES_QUERY)

for item in voting_data:
    voting_map[item[1]][proposals_mapping[item[0]]] = 1

coalitions = [[0 for i in range(15)] for i in range(15)]

for i in range(0, 15):
    for j in range(i + 1, 15):
        coalitions[i][j] = coalitions[j][i] = float(cosine_similarity([voting_map[top_voters[i]]], [voting_map[top_voters[j]]])[0][0])

for item in coalitions:
    print(item)