import requests
import csv

url = "https://hub.snapshot.org/graphql?"
headers = {
    "accept": "application/json",
    "accept-language": "vi,en-US;q=0.9,en;q=0.8",
    "content-type": "application/json",
}

spaces = {}

start_time = 1605388716

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

    # Prepare the request payload
    payload = {
        "query": query,
        "variables": None,
        "operationName": "Spaces",
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        data = response.json()
        fetched_spaces = data.get("data", {}).get("spaces", [])
        ind = 0
        while ind < len(fetched_spaces) and fetched_spaces[ind].get("id", "") in spaces.keys():
            ind += 1
        if ind == len(fetched_spaces):
            break
        print(f"Fetched {len(fetched_spaces) - ind} spaces from {start_time}")
        for item in fetched_spaces:
            id = item.get("id", "")
            if id not in spaces.keys():
                spaces[id] = { "name": item.get("name", ""), "proposalsCount": item.get("proposalsCount", 0) }
        start_time = fetched_spaces[-1].get("created", start_time)
    else:
        print(f"Request failed with status code {response.status_code}: {response.text}")

spaces_data = [(key, value["name"], value["proposalsCount"]) for key, value in spaces.items()]
spaces_data.sort(key=lambda item: item[2], reverse=True)

with open("spaces_by_proposals_count.csv", mode="w", newline="") as file:
    writer = csv.writer(file)
    
    writer.writerow(["id", "name", "proposalsCount"])
    
    writer.writerows(spaces_data)
