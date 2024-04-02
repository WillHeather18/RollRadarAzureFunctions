import requests
import json
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed  # Import for parallel execution
from pymongo import MongoClient
import logging
import numpy as np

# Replace these variables with your actual values

def get_weapon_perks(item_instance_id, destiny_membership_type, destiny_membership_id, headers):
    item_details_url = f"https://www.bungie.net/Platform/Destiny2/{destiny_membership_type}/Profile/{destiny_membership_id}/Item/{item_instance_id}/?components=300,302,304,305,307"
    response = requests.get(item_details_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch item details for instance {item_instance_id}. Status code: {response.status_code}")
        return None
    
def load_weapon_names(db):

    collection = db['WeaponDetails']

    data = collection.find()

    weapon_names_by_id = {weapon['id']: {'name': weapon['name'], 'tierTypeName': weapon['rarity'], 'icon': weapon['iconPath']} for weapon in data}

    return weapon_names_by_id

def fetch_weapon_perks_concurrently(all_weapons, destiny_membership_type, destiny_membership_id, headers, db):
    user_inventory = {}
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_weapon = {executor.submit(get_weapon_perks, weapon['itemInstanceId'], destiny_membership_type, destiny_membership_id, headers): weapon for weapon in all_weapons}
        for future in as_completed(future_to_weapon):
            weapon = future_to_weapon[future]
            try:
                data = future.result()
                if data:
                    user_inventory[weapon['itemInstanceId']] = data
            except Exception as exc:
                print(f"{weapon['itemInstanceId']} generated an exception: {exc}")
                
    return user_inventory


def fetch_destiny_membership_id(bungieId, membershipType ,headers):
    search_url = f"https://www.bungie.net/Platform/User/GetMembershipsById/{bungieId}/{membershipType}/"
    print (search_url)
    response = requests.get(search_url, headers=headers)
    print(response)
    if response.status_code == 200:
        data = response.json()
        return data['Response']['destinyMemberships'][0]['membershipId'], data['Response']['destinyMemberships'][0]['membershipType']
    else:
        raise Exception("Failed to fetch Destiny membership ID")

def fetch_profile_data(destiny_membership_id, destiny_membership_type, headers):
    profile_url = f"https://www.bungie.net/Platform/Destiny2/{destiny_membership_type}/Profile/{destiny_membership_id}/?components=200,102"
    response = requests.get(profile_url, headers=headers)
    if response.status_code == 200:
        print(f"Successfully fetched profile data for Destiny ID: {destiny_membership_id}")
        return response.json()
    
    else:
        raise Exception("Failed to fetch profile data")

def process_weapons_from_data(profile_data, destiny_membership_type, destiny_membership_id, headers, bungieId, db):
    character_ids = profile_data['Response']['characters']['data'].keys()
    vault_items = profile_data['Response']['profileInventory']['data']['items']

    all_weapons = []  # Store tuples or dictionaries of weapon hashes and instance IDs
    character_details = profile_data['Response']['characters']['data']
    
    # Loop through each character ID to fetch both equipped and unequipped items
    for character_id in character_ids:
        character_equipment_url = f"https://www.bungie.net/Platform/Destiny2/{destiny_membership_type}/Profile/{destiny_membership_id}/Character/{character_id}/?components=205"
        character_inventory_url = f"https://www.bungie.net/Platform/Destiny2/{destiny_membership_type}/Profile/{destiny_membership_id}/Character/{character_id}/?components=201"
        
        # Fetch equipped items
        equipment_response = requests.get(character_equipment_url, headers=headers)
        if equipment_response.status_code == 200:
            equipped_items = equipment_response.json().get('Response', {}).get('equipment', {}).get('data', {}).get('items', [])
            for item in equipped_items:
                item_instance_id = item.get('itemInstanceId', 'N/A')
                if item_instance_id != 'N/A':
                    all_weapons.append({'itemHash': item['itemHash'], 'itemInstanceId': item_instance_id})
        else:
            print(f"Failed to fetch equipped items for character {character_id}")

        # Fetch unequipped items (inventory)
        inventory_response = requests.get(character_inventory_url, headers=headers)
        if inventory_response.status_code == 200:
            inventory_items = inventory_response.json().get('Response', {}).get('inventory', {}).get('data', {}).get('items', [])
            for item in inventory_items:
                item_instance_id = item.get('itemInstanceId', 'N/A')
                if item_instance_id != 'N/A':
                    all_weapons.append({'itemHash': item['itemHash'], 'itemInstanceId': item_instance_id})
        else:
            print(f"Failed to fetch inventory items for character {character_id}")
                
    # Process weapons from the vault
    for item in vault_items:
        item_instance_id = item.get('itemInstanceId', 'N/A')
        if item_instance_id != 'N/A':
            all_weapons.append({'itemHash': item['itemHash'], 'itemInstanceId': item_instance_id})  
            
    store_character_details(character_details, bungieId, db)  

    return all_weapons


def store_character_details(character_details, bungieID, db):
    collection = db["UserCharacterDetails"]
    
    class_lookup = {
        0: 'Titan',
        1: 'Hunter',
        2: 'Warlock'
    }
    
    race_lookup = {
        0: 'Human',
        1: 'Awoken',
        2: 'Exo'
    }
    
    gender_lookup = {
        0: 'Male',
        1: 'Female'
    }
    
    for character_id, details in character_details.items():
        if 'classType' in details:
            details['classType'] = class_lookup.get(details['classType'], 'Unknown')
        else:
            details['classType'] = 'Unknown'

        if 'raceType' in details:
            details['raceType'] = race_lookup.get(details['raceType'], 'Unknown')
        else:
            details['raceType'] = 'Unknown'

        if 'genderType' in details:
            details['genderType'] = gender_lookup.get(details['genderType'], 'Unknown')
        else:
            details['genderType'] = 'Unknown'
    
    document = {
        "bungieID": bungieID,
        "character_details" : character_details,
    }
    
    
    collection.replace_one({'bungieID': bungieID}, document, upsert=True)
    
    print(f"Successfully updated character document for bungieID: {bungieID}")

def fetch_and_save_weapon_data(bungieId, membershipType, headers, db):
    try:
        destiny_membership_id, destiny_membership_type = fetch_destiny_membership_id(bungieId, membershipType, headers)


        profile_data = fetch_profile_data(destiny_membership_id, destiny_membership_type, headers)

        all_weapons = process_weapons_from_data(profile_data, destiny_membership_type, destiny_membership_id, headers, bungieId, db)
        
        export_list_to_mongodb(all_weapons, bungieId, destiny_membership_id, db)

        user_inventory = fetch_weapon_perks_concurrently(all_weapons, destiny_membership_type, destiny_membership_id, headers, db)

        if user_inventory is None:
            print(f"Failed to fetch weapon perks for Bungie ID {bungieId}. Skipping...")
        else:
            print(f"Successfully fetched weapon perks for Bungie ID {bungieId}")
        
        return user_inventory, destiny_membership_id
    except Exception as e:
        tb_str = traceback.format_exception(etype=type(e), value=e, tb=e.__traceback__)
        tb_str = "".join(tb_str)  # Convert list of strings into a single string
        print(f"An error occurred in fetch_and_save_weapon_data:\n{tb_str}")
        return None, None
        
def extract_item_details(user_inventory, weapon_details, db):
    extracted_details = []
    for key in user_inventory.keys():
        try:
            
            item_details = user_inventory[key]['Response']['item']['data']
            sockets_details = user_inventory[key]['Response']['sockets']['data']['sockets']
            instance_details = user_inventory[key]['Response']['instance']['data']
            stats_details = user_inventory[key]['Response']['stats']['data']
            characterID = user_inventory[key]['Response'].get('characterId', 0)
                    
            item_hash = item_details.get('itemHash', 'No itemHash found')
            iteminstanceid = item_details.get('itemInstanceId', 'No itemInstanceId found')
            if sockets_details is not None:
                sockets = [sockets_details[i]['plugHash'] if i < len(sockets_details) else 'No plugHash found' for i in range(1, 5)]
            else:
                sockets = ['No plugHash found'] * 4  # Default value if sockets_details is None
            
            # Lookup weapon name and tierTypeName using item_hash
            weapon_info = weapon_details.get(item_hash, None)
            
            if weapon_info:
                extracted_details.append({
                    'itemId': iteminstanceid,
                    'weaponHash': item_hash,
                    'characterId': characterID,
                    'weaponName': weapon_info.get('name', 'N/A'),  # Provide 'N/A' as default value
                    'icon': weapon_info.get('icon', 'N/A'),  # Provide 'N/A' as default value
                    'instance': instance_details if instance_details else 'N/A',  # Provide 'N/A' as default value
                    'socketHashes': sockets if sockets else [],  # Provide empty list as default value
                    'socketsDetails': sockets_details if sockets_details else [],  # Provide empty list as default value
                    'statDetails': stats_details if stats_details else []  # Provide empty list as default value
                })
        except KeyError as e:
            print(f"Error extracting details for key {key}: {e}")
            
    # Remove None values
    
    print(f"Number of weapons: {len(extracted_details)}")
        
    return extracted_details


def process_weapon(invweapon, godrolls):
    weaponHash = int(invweapon['weaponHash'])
    invweapon['score'] = '0/4'  # Initialize the match score out of 4
    total_percentage = 0  # Initialize the total percentage score

    # Find the god roll document that matches the weapon's hash
    for godroll in godrolls.find({'weaponHash': weaponHash}):
        match_count = 0  # Initialize match count for this god roll
        total_weighted_percentage = 0  # Initialize total weighted percentage score
        
        # Iterate through the first 4 socket groups of the god roll
        for socket_group in godroll['sockets_details'][:4]:
            group_weighted_percentage = 0  # Initialize weighted percentage for the current group
            
            for index, socket_option in enumerate(socket_group):
                # Calculate the weight for the current socket based on its position
                weight = 100 - (index * 25)
                weight = max(weight, 0)  # Ensure the weight is not negative
                
                # Extract the socket hash safely
                socket_hash = None
                if 'socketHash' in socket_option:
                    if isinstance(socket_option['socketHash'], dict):
                        socket_hash_key = '$numberLong' if '$numberLong' in socket_option['socketHash'] else '$numberInt'
                        socket_hash = int(socket_option['socketHash'][socket_hash_key])
                    elif isinstance(socket_option['socketHash'], int):
                        socket_hash = socket_option['socketHash']
                
                if socket_hash is not None and socket_hash in invweapon['socketHashes']:
                    match_count += 1 if index == 0 else 0  # Increment match count only for the first option
                    group_weighted_percentage = max(group_weighted_percentage, weight)  # Take the highest weight for this group
            
            total_weighted_percentage += group_weighted_percentage  # Add the group's weight to the total
        
        # Normalize total_weighted_percentage to a scale of 0-100
        total_percentage = (total_weighted_percentage / 400) * 100
        invweapon['score'] = f"{match_count}/4"
        break  # Assuming we only process the first matching god roll

    invweapon['total_percentage'] = total_percentage
    return invweapon


def appraise_inv_parallel(inv, bungieID, destiny_id, db):
    godrolls = db["GodRolls"]

    # Use ThreadPoolExecutor to process each weapon in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Prepare future tasks
        futures = [executor.submit(process_weapon, invweapon, godrolls) for invweapon in inv]

        # Process as each future completes
        for future in as_completed(futures):
            result = future.result()
            if result:
                score = result.get('score')
                if score:
                    x, y = map(int, score.split('/'))
                    result['score_float'] = x / y if y != 0 else 0
                else:
                    result['score_float'] = 0  # Default score for weapons without a score

    inv.sort(key=lambda x: x['score_float'], reverse=True)

    # Process inv as needed
    print(f"Found {len(inv)} weapons with matching sockets. Scores are based on the fraction of matches.")
    for weapon in inv:
        score = weapon.get('score', 'No score')  # Default text for weapons without a score
        
    completedinv ={
        'bungie_id': bungieID,
        'destiny_id': destiny_id,
        'timestamp': datetime.now(),
        'weapons': inv
    }
    
    return completedinv


def export_list_to_mongodb(all_weapons, bungieID, destiny_membership_id, db):
    try:
        collection = db["UserInstanceList"]
        # Assuming you want to store the list of weapons under a specific bungieID
        document = {
            "bungieID": bungieID,
            "destinyID": destiny_membership_id,
            "weapons": all_weapons,  # Store the entire list of weapons as part of the document
            "timestamp": datetime.now()
        }
        collection.replace_one({'bungieID': bungieID}, document, upsert=True)
        print(f"Successfully updated MongoDB document for bungieID: {bungieID}")
    except Exception as e:
        print(f"Failed to export weapon details to MongoDB for bungieID: {bungieID}. Error: {e}")


def export_to_mongodb(details, bungieID, db):
    
    collection = db["UserInventory"]
            
    collection.replace_one({'bungie_id': bungieID}, details, upsert=True)
    
    print(f"Inserted {len(details)} weapons into MongoDB.")

def process_user_inventory(bungieID, membershipType, access_token, weapon_details, db, api_key):
    headers = {
        'X-API-Key': api_key,
        'Authorization': f'Bearer {access_token}'
    }
    
    print(f"Fetching inventory for Bungie ID {bungieID}")
    user_inventory, destiny_id = fetch_and_save_weapon_data(bungieID, membershipType, headers, db)

    if user_inventory is None:
        print(f"Failed to fetch inventory for Bungie ID {bungieID}. Skipping...")
        return  # Early return if user_inventory is None
    
    sanitised_inventory = extract_item_details(user_inventory, weapon_details, db)
    appraised_inventory = appraise_inv_parallel(sanitised_inventory, bungieID, destiny_id, db)
    export_to_mongodb(appraised_inventory, bungieID, db)



def GetInventory(db, api_key):
    
    print("Starting inventory processing...")
    
    testID = '12191971'
    
    collection = db["UserDetails"] 
    documents = list(collection.find({'bungie_id': testID}))  # Only get the document with the matching bungie_id
    
    weapon_details = load_weapon_names(db)  # Load weapon names into memory

    with ThreadPoolExecutor(max_workers=10) as executor:
        # Submit all user inventory processing tasks to the executor
        future_to_bungieID = {executor.submit(process_user_inventory, doc['bungie_id'], doc['membership_type'], doc['access_token'], weapon_details, db, api_key): doc for doc in documents}
        
        for future in as_completed(future_to_bungieID):
            bungieID = future_to_bungieID[future]['bungie_id']
            try:
                # Here, you could do something with the result if needed
                future.result()
                print(f"Successfully processed inventory for Bungie ID {bungieID}")
            except Exception as exc:
                tb_str = traceback.format_exception(type(exc), exc, exc.__traceback__)
                tb_str = "".join(tb_str)  # Convert list of strings into a single string
                logging.error(f"An error occurred while processing inventory for Bungie ID {bungieID}:\n{tb_str}")


    print(f"Processed {len(documents)} documents.")

