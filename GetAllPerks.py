import sqlite3
import json
import logging

def get_perks_from_inventory(db_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    perk_hashes = [
    7906839, 2833605196, 1806783418, 2718120384, 2619833294, 3962145884, 
    577918720, 1257608559, 1757026848, 683359327, 1041766312, 3809303875, 
    1697972157, 1202604782
    ]

    # Prepare the query to search for items with the specified plugCategoryHashes
    # Join the perk_hashes into a string for the SQL query
    perk_hashes_str = ','.join(str(hash) for hash in perk_hashes)

    query = f"""
    SELECT json
    FROM DestinyInventoryItemDefinition
    WHERE json_extract(json, '$.plug.plugCategoryHash') IN ({perk_hashes_str});
    """

    perks_data = []
    try:
        cursor.execute(query)
        # Fetch all rows, each row's first (and only) item is the item's full JSON data
        rows = cursor.fetchall()
        for row in rows:
            # Parse the JSON data and append it to the perks_data list
            perks_data.append(json.loads(row[0]))
    finally:
        # Make sure to close the database connection
        conn.close()

    return perks_data

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

    perks_data = get_perks_from_inventory(db_path)

    logging.info(f"Retrieved {len(perks_data)} perks from the Destiny 2 Manifest")
    
    save_perks_to_mongodb(perks_data, db)
    
    return perks_data

