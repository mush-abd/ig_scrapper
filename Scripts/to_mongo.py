from apify_client import ApifyClient
from pymongo import MongoClient
import json
import config

# ─────────────────────────────────────────────
# MONGODB CONFIGURATION
# ─────────────────────────────────────────────
MONGO_ENABLED = True                          # Set to False to skip MongoDB
MONGO_URI = config.mongo_uri                  # e.g. "mongodb://localhost:27017" or Atlas URI
MONGO_DB_NAME = config.mongo_db_name          # Target database
MONGO_COLLECTION = config.mongo_collection_name                 # Target collection

# Keys to extract from each scraped item and insert into MongoDB.
# Set to None to insert ALL keys.
SELECTED_KEYS = [
    "inputUrl",
    "username",
    "fullName",
    "biography",
    "externalUrl",
    "followersCount",
    "followsCount",
    "businessCategoryName",
    "profilePicUrlHD",
    "postsCount",
    "latestPosts",
]
# ─────────────────────────────────────────────


def connect_mongo(uri: str, db_name: str, collection_name: str):
    """Connect to MongoDB and return the target collection."""
    mongo_client = MongoClient(uri)
    db = mongo_client[db_name]
    collection = db[collection_name]
    print(f"[MongoDB] Connected → {db_name}.{collection_name}")
    return mongo_client, collection


def filter_keys(item: dict, keys: list | None) -> dict:
    """Return only the selected keys from item, or the full item if keys is None."""
    if keys is None:
        return item
    return {k: item[k] for k in keys if k in item}


def upsert_creator(collection, document: dict):
    """
    Upsert a creator document into MongoDB.
    Uses 'username' as the unique identifier; falls back to 'id'.
    """
    unique_field = "username" if "username" in document else "id"
    if unique_field not in document:
        # No unique key — plain insert
        result = collection.insert_one(document)
        print(f"[MongoDB] Inserted new document: {result.inserted_id}")
    else:
        result = collection.update_one(
            {unique_field: document[unique_field]},
            {"$set": document},
            upsert=True,
        )
        if result.upserted_id:
            print(f"[MongoDB] Created creator '{document[unique_field]}' ({result.upserted_id})")
        else:
            print(f"[MongoDB] Updated creator '{document[unique_field]}'")


# ─── Apify setup ──────────────────────────────
apify_client = ApifyClient(config.apifY_access_id)

run_input = {
    "directUrls": ["https://www.instagram.com/nike", "https://www.instagram.com/puma"],
    "resultsType": "details",
    "resultsLimit": 1,
    "onlyPostsNewerThan": "2025-02-01",
}

run = apify_client.actor("shu8hvrXbJbY3Eb9W").call(run_input=run_input)

# ─── MongoDB connection (once, before iterating) ──
mongo_client = None
collection = None
if MONGO_ENABLED:
    mongo_client, collection = connect_mongo(MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION)

# ─── Process results ──────────────────────────
try:
    for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
        item = dict(item)

        # Save to local JSON file
        with open("output.jsonl", "a") as json_file:
            json_file.write(json.dumps(item) + "\n")

        print(item)

        # Insert / update in MongoDB
        if MONGO_ENABLED and collection is not None:
            filtered = filter_keys(item, SELECTED_KEYS)
            upsert_creator(collection, filtered)

finally:
    if mongo_client:
        mongo_client.close()
        print("[MongoDB] Connection closed.")