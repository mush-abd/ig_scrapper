
from apify_client import ApifyClient
from pymongo import MongoClient, ASCENDING
import json
import config

# ─────────────────────────────────────────────
# MONGODB CONFIGURATION
# ─────────────────────────────────────────────
MONGO_ENABLED = True
MONGO_URI = config.mongo_uri
MONGO_DB_NAME = config.mongo_db_name
MONGO_COLLECTION = "comments"

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


# Keys to extract from each post and store in MongoDB.
# Set to None to store ALL keys.
SELECTED_KEYS = [
    "id",
    "inputUrl",
    "ownerUsername",
    "ownerFullName",
    "ownerId",
    "url",
    "shortCode",
    "type",
    "timestamp",
    "caption",
    "hashtags",
    "mentions",
    "taggedUsers",
    "likesCount",
    "commentsCount",
    "videoViewCount",
    "videoPlayCount",
    "videoDuration",
    "displayUrl",
    "videoUrl",
    "images",
    "locationName",
    "locationId",
    "paidPartnership",
    "isPinned",
    "isCommentsDisabled",
    "latestComments",
    "firstComment",
    "musicInfo",
    "childPosts",
]
# ─────────────────────────────────────────────


def connect_mongo(uri: str, db_name: str, collection_name: str):
    """Connect to MongoDB, return client and collection."""
    mongo_client = MongoClient(uri)
    db = mongo_client[db_name]
    collection = db[collection_name]

    # Ensure a unique index on post 'id' to prevent duplicates
    collection.create_index([("id", ASCENDING)], unique=True)
    print(f"[MongoDB] Connected → {db_name}.{collection_name}")
    return mongo_client, collection


def filter_keys(item: dict, keys: list | None) -> dict:
    """Return only selected keys from item, or the full item if keys is None."""
    if keys is None:
        return item
    return {k: item[k] for k in keys if k in item}


def upsert_post(collection, document: dict):
    """
    Upsert a post document using Instagram's post 'id' as the unique key.
    This prevents duplicate posts if the scraper is re-run.
    """
    if "id" not in document:
        result = collection.insert_one(document)
        print(f"[MongoDB] Inserted post (no id): {result.inserted_id}")
        return

    result = collection.update_one(
        {"id": document["id"]},
        {"$set": document},
        upsert=True,
    )
    if result.upserted_id:
        print(f"[MongoDB] Created post '{document.get('shortCode', document['id'])}' by @{document.get('ownerUsername', '?')}")
    else:
        print(f"[MongoDB] Updated post '{document.get('shortCode', document['id'])}' by @{document.get('ownerUsername', '?')}")


# ─── Apify setup ──────────────────────────────
apify_client = ApifyClient(config.apifY_access_id)

run_input = {
    "directUrls": [
        "https://www.instagram.com/puma",
        "https://www.instagram.com/nike",
        "https://www.instagram.com/adidas",
        "https://www.instagram.com/reebok",
        "https://www.instagram.com/underarmour",
        "https://www.instagram.com/newbalance",
    ],
    "resultsType": "posts",
    "resultsLimit": 10,
    "onlyPostsNewerThan": "2025-02-01",
}

run = apify_client.actor("shu8hvrXbJbY3Eb9W").call(run_input=run_input)

# ─── MongoDB connection ────────────────────────
mongo_client = None
collection = None
if MONGO_ENABLED:
    mongo_client, collection = connect_mongo(MONGO_URI, MONGO_DB_NAME, MONGO_COLLECTION)

# ─── Process results ──────────────────────────
try:
    for item in apify_client.dataset(run["defaultDatasetId"]).iterate_items():
        item = dict(item)

        # Save to local JSONL file
        with open("posts.jsonl", "a") as f:
            f.write(json.dumps(item) + "\n")

        print(item)

        # Insert / update in MongoDB
        if MONGO_ENABLED and collection is not None:
            filtered = filter_keys(item, SELECTED_KEYS)
            upsert_post(collection, filtered)

finally:
    if mongo_client:
        mongo_client.close()
        print("[MongoDB] Connection closed.")
