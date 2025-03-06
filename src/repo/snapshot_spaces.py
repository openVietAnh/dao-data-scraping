from src.db.sql import SQL
from src.graphql.snapshot_request import request

def get_snapshot_spaces():
    db_connection = SQL()
    start_time, spaces = 0, set()

    while True:
        query = """
            query Spaces {
            spaces(
                first: 1000,
                skip: 0,
                where: {
                    created_gte: """ + str(start_time) + """
                }
                orderBy: "created",
                orderDirection: asc
            ) {
                id
                name
                created
                proposalsCount
            }
            }
        """

        response = request(query, "Spaces")

        if response.status_code == 200:
            data = response.json()
            fetched_spaces = data.get("data", {}).get("spaces", [])
            ind = 0
            while ind < len(fetched_spaces) and fetched_spaces[ind].get("id", "") in spaces:
                ind += 1
            if ind == len(fetched_spaces):
                break
            spaces.add(map(lambda space: space["id"], fetched_spaces[ind:]))
            print(f"Fetched {len(fetched_spaces) - ind} spaces from {start_time}")
            values = [(
                space["id"],
                space["name"],
                int(space["created"]),
                int(space["proposalsCount"])
            ) for space in fetched_spaces[ind:]]
            db_connection.execute_many("""
                INSERT INTO spaces (id, name, created, proposalsCount)
                VALUES (%s, %s, FROM_UNIXTIME(%s), %s)
                ON DUPLICATE KEY UPDATE 
                    id = VALUES(id), 
                    name = VALUES(name), 
                    created = VALUES(created), 
                    proposalsCount = VALUES(proposalsCount) 
            """, values)
            if len(fetched_spaces) < 1000:
                break
            start_time = fetched_spaces[-1].get("created", start_time)
        else:
            print(f"Request failed with status code {response.status_code}: {response.text}")
            break

    db_connection.close()
