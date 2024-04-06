import sqlite3
import json
import logging

def get_perks_from_inventory(conn):
    # Connect to the SQLite database
    cursor = conn.cursor()

    # List of hashes to check for
    hashes = [610365472, 141186804]  # replace with your actual hashes

    # Create a list of LIKE conditions for each hash
    like_conditions = [f"json_extract(json, '$.itemCategoryHashes') LIKE '%{hash}%'" for hash in hashes]

    # Join the conditions with OR to create the WHERE clause
    where_clause = " OR ".join(like_conditions)

    query = f"""
    SELECT json
    FROM DestinyInventoryItemDefinition
    WHERE {where_clause};
    """

    items_data = []
    try:
        cursor.execute(query)
        # Fetch all rows, each row's first (and only) item is the item's full JSON data
        rows = cursor.fetchall()
        for row in rows:
            # Parse the JSON data and append it to the items_data list
            items_data.append(json.loads(row[0]))
    finally:
        # Make sure to close the database connection
        conn.close()

    return items_data


def save_perks_to_mongodb(perks_data, db):
    
    collection = db["PerkDetails"]
    
    # Insert the weapon data into the MongoDB collection
    collection.delete_many({})
    collection.insert_many(perks_data)
    
    logging.info("Perk data saved to MongoDB successfully!")

# Path to your Destiny 2 Manifest database
def GetPerks(tempfile_path, db):
    db_path = tempfile_path
# Specify the path for the JSON file where the perks data will be stored
    conn = sqlite3.connect(db_path)

    perks_data = get_perks_from_inventory(conn)

    logging.info(f"Retrieved {len(perks_data)} perks from the Destiny 2 Manifest")
    
    save_perks_to_mongodb(perks_data, db)
    
    return perks_data

