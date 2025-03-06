import requests

REQUEST_URL = "https://hub.snapshot.org/graphql?"
HEADERS = {"accept": "application/json", "content-type": "application/json"}

def request(query, operarion_name):
    response = requests.post(
        REQUEST_URL,
        headers=HEADERS,
        json={"query": query, "variables": None, "operationName": operarion_name})
    return response