from apify_client import ApifyClient
import pickle
import json
import config
# Initialize the ApifyClient with your API token
client = ApifyClient(config.apifY_access_id)

# Prepare the Actor input
run_input = {
    "directUrls": ["https://www.instagram.com/puma", "https://www.instagram.com/nike", "https://www.instagram.com/adidas", "https://www.instagram.com/reebok", "https://www.instagram.com/underarmour", "https://www.instagram.com/newbalance", "https://www.instagram.com/asics", "https://www.instagram.com/converse", "https://www.instagram.com/vans", "https://www.instagram.com/saucony"],
    "resultsType": "posts",
    "resultsLimit": 20,
    "onlyPostsNewerThan": "2025-02-01"
}

# Run the Actor and wait for it to finish
run = client.actor("shu8hvrXbJbY3Eb9W").call(run_input=run_input)

# Fetch and print Actor results from the run's dataset (if there are any)
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    item = dict(item)  # Convert the item to a regular dictionary
    json_string = json.dumps(item)  # Convert the dictionary to a JSON string
    with open("posts.jsonl", "a") as json_file:
        json_file.write(json_string + "\n")  # Write the JSON string to a file with a newline
    print(item)

