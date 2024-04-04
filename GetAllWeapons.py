import sqlite3
import logging
import json

stat_name_lookup = {
    2223994109: "Aspect Energy Capacity",
    2341766298: "Handicap",
    2399985800: "Void Cost",
    2441327376: "Armor Energy Capacity",
    2523465841: "Velocity",
    2714457168: "Airborne Effectiveness",
    2715839340: "Recoil Direction",
    2733264856: "Score Multiplier",
    2762071195: "Guard Efficiency",
    2837207746: "Swing Speed",
    2961396640: "Charge Time",
    2996146975: "Mobility",
    3017642079: "Boost",
    3022301683: "Charge Rate",
    3289069874: "Power Bonus",
    3344745325: "Solar Cost",
    3555269338: "Zoom",
    3578062600: "Any Energy Type Cost",
    3597844532: "Precision Damage",
    3614673599: "Blast Radius",
    3625423501: "Armor Energy Capacity",
    3736848092: "Guard Endurance",
    3779394102: "Arc Cost",
    3871231066: "Magazine",
    3897883278: "Defense",
    3907551967: "Move Speed",
    3950461274: "Armor Energy Capacity",
    3988418950: "Time to Aim Down Sights",
    4043523819: "Impact",
    4188031367: "Reload Speed",
    4244567218: "Strength",
    4284893193: "Rounds Per Minute",
    16120457: "Armor Energy Capacity",
    119204074: "Fragment Cost",
    144602215: "Intellect",
    155624089: "Stability",
    209426660: "Guard Resistance",
    237763788: "Ghost Energy Capacity",
    360359141: "Durability",
    392767087: "Resilience",
    447667954: "Draw Time",
    514071887: "Mod Cost",
    925767036: "Ammo Capacity",
    943549884: "Handling",
    998798867: "Stasis Cost",
    1240592695: "Range",
    1345609583: "Aim Assistance",
    1480404414: "Attack",
    1501155019: "Speed",
    1546607977: "Heroic Resistance",
    1546607978: "Arc Damage Resistance",
    1546607979: "Solar Damage Resistance",
    1546607980: "Void Damage Resistance",
    1591432999: "Accuracy",
    1735777505: "Discipline",
    1842278586: "Shield Duration",
    1931675084: "Inventory Size",
    1935470627: "Power",
    1943323491: "Recovery",
    2018193158: "Armor Energy Capacity"
}

damage_name_lookup = {
    '1': ("Kinetic", "/common/destiny2_content/icons/DestinyDamageTypeDefinition_3385a924fd3ccb92c343ade19f19a370.png"),
    '2': ("Arc", "/common/destiny2_content/icons/DestinyDamageTypeDefinition_092d066688b879c807c3b460afdd61e6.png"),
    '3': ("Solar", "/common/destiny2_content/icons/DestinyDamageTypeDefinition_2a1773e10968f2d088b97c22b22bba9e.png"),
    '4': ("Void", "/common/destiny2_content/icons/DestinyDamageTypeDefinition_ceb2f6197dccf3958bb31cc783eb97a0.png"),
    '6': ("Stasis", "/common/destiny2_content/icons/DestinyDamageTypeDefinition_530c4c3e7981dc2aefd24fd3293482bf.png"),
    '7': ("Strand", "/common/destiny2_content/icons/DestinyDamageTypeDefinition_b2fe51a94f3533f97079dfa0d27a4096.png")
}

ammo_name_lookup = {
    1: "Primary",
    2: "Special",
    3: "Heavy"
}

def get_weapon_names_and_ids(db_path):
    # Connect to the SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Query to select weapon names and their ids (hashes)
    query = """
        SELECT DISTINCT 
        json_extract(json, '$.hash') AS ItemHash, 
        json_extract(json, '$.displayProperties.name') AS Name, 
        json_extract(json, '$.itemTypeDisplayName') AS Type, 
        json_extract(json, '$.inventory.tierTypeName') AS Rarity, 
        json_extract(json, '$.damageTypes') AS DamageTypes, 
        json_extract(json, '$.stats.stats') AS Stats,
        json_extract(json, '$.equippingBlock.ammoType') AS AmmoType,
        json_extract(json, '$.inventory.bucketTypeHash') AS WeaponSlot,
        json_extract(json, '$.displaySource') AS AcquisitionSource,
        json_extract(json, '$.loreHash') AS LoreHash,
        json_extract(json, '$.displayProperties.icon') AS IconPath, 
        json_extract(json, '$.iconWatermark') AS WatermarkPath,
        json_extract(json, '$.screenshot') AS ScreenshotPath,
        json_extract(json, '$.sockets') AS Sockets
        FROM DestinyInventoryItemDefinition 
        WHERE json_extract(json, '$.itemType') = 3 
        AND json_extract(json, '$.displayProperties.name') IS NOT NULL 
        ORDER BY json_extract(json, '$.displayProperties.name');
        """

    weapons = []
    try:
        cursor.execute(query)
        weapons = cursor.fetchall()
    finally:
        # Make sure to close the database connection
        conn.close()
    
    print(f"Found weapons details for {len(weapons)} weapons.")
    return weapons

def save_weapons_to_mongodb(weapons, db):
    
    collection = db["WeaponDetails"]
    
    # Insert the weapon data into the MongoDB collection
    collection.delete_many({})

    
    collection.insert_many(weapons)
    
    print(f"Inserted {len(weapons)} weapons into MongoDB")
    
    
import json

def process_hashes(weapons):
    processed_weapons = []
    for weapon in weapons:
        # Initialize the weapon dictionary directly from the weapon tuple
        weapon_dict = {
            "id": weapon[0], 
            "name": weapon[1], 
            "type": weapon[2], 
            "rarity": weapon[3], 
            "damageTypes": weapon[4], 
            "stats": weapon[5],
            "ammoType": weapon[6], 
            "weaponSlot": weapon[7],
            "acquisitionSource": weapon[8], 
            "loreHash": weapon[9], 
            "iconPath": weapon[10], 
            "watermarkPath": weapon[11],
            "screenshotPath": weapon[12],
            "socket_types": weapon[13],
            "randomRoll": False
        }

        # Attempt to decode the JSON string for stats
        try:
            weapon_stats_data = json.loads(weapon_dict["stats"])
        except json.JSONDecodeError:
            print(f"Error decoding JSON for weapon: {weapon_dict['name']}")
            continue  # Skip this weapon if there's a problem with the stats JSON

        # Process the stats to convert stat hashes to names
        processed_stats = []
        for stat_hash, stat_info in weapon_stats_data.items():
            stat_hash_int = int(stat_hash)  # Convert hash to integer for lookup
            stat_name = stat_name_lookup.get(stat_hash_int, None)
            if stat_name is None:
                continue  # Skip this stat if the name is unknown
            value = stat_info.get('value', 'N/A')
            minimum = stat_info.get('minimum', 'N/A')
            maximum = stat_info.get('maximum', 'N/A')
            display_maximum = stat_info.get('displayMaximum', 'N/A')
            processed_stats.append({
                'stat_hash': stat_hash_int,
                'stat_name': stat_name,
                'value': value,
                'minimum': minimum,
                'maximum': maximum,
                'display_maximum': display_maximum
            })
            
        processed_damage_types = []
        damage_types = json.loads(weapon_dict["damageTypes"])
        for damage_type in damage_types:
            damage_type_info = damage_name_lookup.get(str(damage_type), f"Unknown Damage Type: {damage_type}")
            # Convert the tuple to a list and append it
            processed_damage_types.extend(damage_type_info)
                      
        ammo_type = weapon_dict["ammoType"]
        ammo_type_name = ammo_name_lookup.get(ammo_type, f"Unknown Ammo Type: {ammo_type}")
        
        processed_socket_types = []
        random_roll = False
        
        # Attempt to decode the JSON string for sockets
        try:
            socket_data = json.loads(weapon_dict["socket_types"])
            for socket_entry in socket_data.get('socketEntries', []):
                socket_type_hash = socket_entry.get('socketTypeHash')
                if socket_type_hash is not None:
                    processed_socket_types.append(socket_type_hash)
                if 'randomizedPlugSetHash' in socket_entry:
                    random_roll = True
        except json.JSONDecodeError:
            print(f"Error decoding JSON for sockets in weapon: {weapon_dict['name']}")
            
        weaponSlot = weapon_dict["weaponSlot"]
        
        if weaponSlot == 1498876634:
            weaponSlot = "Primary"
        elif weaponSlot == 2465295065:
            weaponSlot = "Secondary"
        elif weaponSlot == 953998645:
            weaponSlot = "Heavy"
            
            
        # Update weapon_dict with the processed stats
        weapon_dict["stats"] = processed_stats
        weapon_dict["damageTypes"] = processed_damage_types
        weapon_dict["ammoType"] = ammo_type_name
        weapon_dict["socket_types"] = processed_socket_types
        weapon_dict["weaponSlot"] = weaponSlot
        weapon_dict["randomRoll"] = random_roll
        
        # Append the updated dictionary to processed_weapons
        processed_weapons.append(weapon_dict)
        

    return processed_weapons

def GetWeapons(temp_file_path, db):
    # Path to your Destiny 2 Manifest database
    db_path = temp_file_path
    # Specify the path for the JSON file where the weapon names and ids will be stored

    weapons = get_weapon_names_and_ids(db_path)
    weapons_processed = process_hashes(weapons)
    save_weapons_to_mongodb(weapons_processed, db)
    logging.info(f"Saved {len(weapons)} weapons to MongoDB")
    return weapons