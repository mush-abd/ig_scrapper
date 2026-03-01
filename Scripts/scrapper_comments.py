from apify_client import ApifyClient
import pickle
import json
import config
from pymongo import MongoClient, ASCENDING
# Initialize the ApifyClient with your API token
client = ApifyClient(config.apifY_access_id)

MONGO_ENABLED = True
MONGO_URI = config.mongo_uri
MONGO_DB_NAME = config.mongo_db_name
MONGO_COLLECTION = "posts"


def get_all_post_urls(
    uri: str = config.mongo_uri,
    db_name: str = "your_database_name",
    collection_name: str = "posts",
    url_field: str = "url"  # adjust to whatever the field is named in your posts documents
) -> list[str]:
    """
    Connects to MongoDB and returns a list of all post URLs
    from the specified collection.
    """
    client = MongoClient(uri)
    try:
        collection = client[db_name][collection_name]
        urls = [
            doc[url_field]
            for doc in collection.find({}, {url_field: 1, "_id": 0})
            if url_field in doc
        ]
        print(f"[MongoDB] Retrieved {len(urls)} post URLs.")
        return urls
    finally:
        client.close()

comments_list = get_all_post_urls(MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION)

# Prepare the Actor input
run_input = {
    "directUrls": comments_list,
    "resultsType": "comments",
    "resultsLimit": 20,
    "onlyPostsNewerThan": "2025-02-01"
}

# Run the Actor and wait for it to finish
run = client.actor("shu8hvrXbJbY3Eb9W").call(run_input=run_input)

# Fetch and print Actor results from the run's dataset (if there are any)
for item in client.dataset(run["defaultDatasetId"]).iterate_items():
    item = dict(item)  # Convert the item to a regular dictionary
    json_string = json.dumps(item)  # Convert the dictionary to a JSON string
    with open("comments.jsonl", "a") as json_file:
        json_file.write(json_string + "\n")  # Write the JSON string to a file with a newline
    print(item)
