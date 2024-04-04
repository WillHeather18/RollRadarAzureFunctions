import aiohttp
import asyncio
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed  # Import for parallel execution
import logging
import os

# Replace these variables with your actual values

api_key = os.environ["API_KEY"]

async def get_weapon_perks(item_instance_id, destiny_membership_type, destiny_membership_id, session):
    item_details_url = f"https://www.bungie.net/Platform/Destiny2/{destiny_membership_type}/Profile/{destiny_membership_id}/Item/{item_instance_id}/?components=300,302,304,305,307"
    async with session.get(item_details_url) as response:
        if response.status == 200:
            return await response.json()
        else:
            print(f"Failed to fetch item details for instance {item_instance_id}. Status code: {response.status}")
            return None
    
def load_weapon_names(db):

    collection = db['WeaponDetails']

    data = collection.find()

    weapon_names_by_id = {weapon['id']: {'name': weapon['name'], 'tierTypeName': weapon['rarity'], 'icon': weapon['iconPath']} for weapon in data}

    return weapon_names_by_id

async def fetch_weapon_perks_concurrently(all_weapons, destiny_membership_type, destiny_membership_id, session):
    user_inventory = {}
    tasks = [get_weapon_perks(weapon['itemInstanceId'], destiny_membership_type, destiny_membership_id, session) for weapon in all_weapons]
    results = await asyncio.gather(*tasks)
    for result, weapon in zip(results, all_weapons):
        if result:
            user_inventory[weapon['itemInstanceId']] = result
    return user_inventory

async def fetch_profile_data(destiny_membership_id, destiny_membership_type, session):
    profile_url = f"https://www.bungie.net/Platform/Destiny2/{destiny_membership_type}/Profile/{destiny_membership_id}/?components=200,102"
    async with session.get(profile_url) as response:
        if response.status == 200:
            print(f"Successfully fetched profile data for Destiny ID: {destiny_membership_id}")
            return await response.json()
        
        else:
            raise Exception("Failed to fetch profile data")


async def process_weapons_from_data(profile_data, destiny_membership_type, destiny_membership_id, session, bungieId, db):
    character_ids = profile_data['Response']['characters']['data'].keys()
    vault_items = profile_data['Response']['profileInventory']['data']['items']

    all_weapons = []  # Store tuples or dictionaries of weapon hashes and instance IDs
    character_details = profile_data['Response']['characters']['data']

    async def fetch_items(url):
        async with session.get(url) as response:
            if response.status == 200:
                data = await response.json()
                return data
            else:
                print(f"Failed to fetch data from {url}. Status code: {response.status}")
                return None

    # Fetch items for each character and the vault asynchronously
    tasks = []
    for character_id in character_ids:
        equipment_url = f"https://www.bungie.net/Platform/Destiny2/{destiny_membership_type}/Profile/{destiny_membership_id}/Character/{character_id}/?components=205"
        inventory_url = f"https://www.bungie.net/Platform/Destiny2/{destiny_membership_type}/Profile/{destiny_membership_id}/Character/{character_id}/?components=201"
        tasks.append(fetch_items(equipment_url))
        tasks.append(fetch_items(inventory_url))

    responses = await asyncio.gather(*tasks)

    for response in responses:
        if response:
            items = response.get('Response', {}).get('equipment', {}).get('data', {}).get('items', []) + \
                    response.get('Response', {}).get('inventory', {}).get('data', {}).get('items', [])
            for item in items:
                item_instance_id = item.get('itemInstanceId', 'N/A')
                if item_instance_id != 'N/A':
                    all_weapons.append({'itemHash': item['itemHash'], 'itemInstanceId': item_instance_id})

    # Process weapons from the vault
    for item in vault_items:
        item_instance_id = item.get('itemInstanceId', 'N/A')
        if item_instance_id != 'N/A':
            all_weapons.append({'itemHash': item['itemHash'], 'itemInstanceId': item_instance_id})

    async_store_character_details(character_details, bungieId, db)

    return all_weapons

async def async_store_character_details(character_details, bungieID, db):
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None,  # Default executor (ThreadPoolExecutor)
        store_character_details,  # Synchronous function
        character_details, bungieID, db  # Arguments to the function
    )
    print(f"Successfully updated character document for bungieID: {bungieID} in async manner")

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

async def fetch_and_save_weapon_data(bungieId, membershipType, destiny_membership_id,session, db):
    try:
        profile_data = await fetch_profile_data(destiny_membership_id, membershipType, session)
        if profile_data is None:
            print(f"Failed to fetch profile data for Bungie ID {bungieId}. Skipping...")
        else:
            print(f"Successfully fetched profile data for Bungie ID {bungieId}")
        
        all_weapons = await process_weapons_from_data(profile_data, membershipType, destiny_membership_id, session, bungieId, db)
        
        # Assuming export_list_to_mongodb is updated to async or handled properly if it's synchronous
        export_list_to_mongodb(all_weapons, bungieId, destiny_membership_id, db)

        user_inventory = await fetch_weapon_perks_concurrently(all_weapons, membershipType, destiny_membership_id, session)

        if user_inventory is None:
            print(f"Failed to fetch weapon perks for Bungie ID {bungieId}. Skipping...")
        else:
            print(f"Successfully fetched weapon perks for Bungie ID {bungieId}")
        
        return user_inventory
    
    except Exception as e:
        tb_str = traceback.format_exception(type(e), e, e.__traceback__)
        tb_str = "".join(tb_str)  # Convert list of strings into a single string
        print(f"An error occurred in fetch_and_save_weapon_data:\n{tb_str}")
        print(user_inventory)
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
        # Assuming `db` is already an instance of AsyncIOMotorClient's database
        collection = db["UserInstanceList"]
        
        document = {
            "bungieID": bungieID,
            "destinyID": destiny_membership_id,
            "weapons": all_weapons,
            "timestamp": datetime.now()
        }
        
        # Using the asynchronous replace_one method provided by motor
        collection.replace_one({'bungieID': bungieID}, document, upsert=True)
        print(f"Successfully updated MongoDB document for bungieID: {bungieID}")
    except Exception as e:
        print(f"Failed to export weapon details to MongoDB for bungieID: {bungieID}. Error: {e}")


def export_to_mongodb(details, bungieID, db):
    
    collection = db["UserInventory"]
            
    collection.replace_one({'bungie_id': bungieID}, details, upsert=True)
    
    print(f"Inserted {len(details)} weapons into MongoDB.")

async def process_user_inventory(bungieID, membershipType, destiny_membership_id,weapon_details, db, session):
    print(f"Fetching inventory for Bungie ID {bungieID}")
    user_inventory = await fetch_and_save_weapon_data(bungieID, membershipType, destiny_membership_id,session, db)

    if user_inventory is None:
        print(f"Failed to fetch inventory for Bungie ID {bungieID}. Skipping...")
        return  # Early return if user_inventory is None
    
    sanitised_inventory = extract_item_details(user_inventory, weapon_details, db)
    appraised_inventory = appraise_inv_parallel(sanitised_inventory, bungieID, destiny_membership_id, db)
    export_to_mongodb(appraised_inventory, bungieID, db)
    
    return appraised_inventory



async def GetInventory(db, bungieID, membershipType, destiny_membership_id,access_token):
    print("Starting inventory processing...")
    
    print("Bungie ID: ", bungieID)
    print("Membership Type: ", membershipType)
    print("Destiny Membership ID: ", destiny_membership_id)
    
    async with aiohttp.ClientSession(headers={
        'X-API-Key': api_key,
        'Authorization': f'Bearer {access_token}'
    }) as session:

        # Load weapon names into memory
        weapon_details = load_weapon_names(db)

        # Process the inventory for the single user
        try:
            appraised_inv = await process_user_inventory(bungieID, membershipType, destiny_membership_id,weapon_details, db, session)
            print(f"Successfully processed inventory for Bungie ID {bungieID}")
            return appraised_inv
        except Exception as exc:
            tb_str = traceback.format_exception(type(exc), exc, exc.__traceback__)
            tb_str = "".join(tb_str)  # Convert list of strings into a single string
            logging.error(f"An error occurred while processing inventory for Bungie ID {bungieID}:\n{tb_str}")


