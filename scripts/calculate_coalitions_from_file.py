from sklearn.metrics.pairwise import cosine_similarity
from datetime import datetime,timedelta

FILE_NAME = "opcollective.eth_voting_map.csv"
SPACE_ID = "opcollective.eth"

voting_map = {}
VOTERS_LIMIT = 3666

with open(FILE_NAME, "r") as csvfile:
    data = csvfile.readlines()
    voters_num = VOTERS_LIMIT
    for i, line in enumerate(data[:voters_num]):
        voting_map[i] = list(map(int, line.strip().split(',')))

coalitions = [[1 for i in range(voters_num)] for i in range(voters_num)]
count = 0

total = voters_num * (voters_num - 1) // 2

start_time = datetime.now()
for i in range(0, voters_num):
    for j in range(i + 1, voters_num):
        coalitions[i][j] = coalitions[j][i] = float(cosine_similarity([voting_map[i]], [voting_map[j]])[0][0])
        count += 1

        elapsed_time = datetime.now() - start_time
        avg_time_per_iteration = elapsed_time / count
        remaining_iterations = total - count
        estimated_completion_time = datetime.now() + avg_time_per_iteration * remaining_iterations

        print(f"[{datetime.now()}] Calculated {i}-{j};Estimated completion time: {estimated_completion_time.strftime('%H:%M:%S')}")

with open(f"{SPACE_ID}_coalitions.csv", "w") as f:
    for item in coalitions:
        f.write(f"{",".join(map(lambda x: str(x), item))}\n")

print("Processing complete.")