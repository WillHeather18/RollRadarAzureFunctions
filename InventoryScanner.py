import logging
import requests
from pymongo import MongoClient
from pyfcm import FCMNotification
import os

logger = logging.getLogger('azure')
logger.setLevel(logging.INFO)

MONGODB_URI = os.environ['MONGODB_URI']  # MongoDB connection string
DB_NAME = 'RollRadar'  # MongoDB database name
STORAGE_CONNECTION_STRING = os.environ['AzureWebJobsStorage']  # Azure Storage connection string
QUEUE_NAME = 'user-check-queue'  # Azure Queue name
API_KEY = os.environ['API_KEY']  # Bungie API key
CLIENT_SECRET = os.environ['CLIENT_SECRET']  # Bungie client secret
CLIENT_ID = os.environ['CLIENT_ID']  # Bungie client ID
FCM_API_KEY = os.environ['FCM_API_KEY']  # Firebase Cloud Messaging API key

client = MongoClient(MONGODB_URI)

db = client['RollRadar']

def ProcessQueueMessage(userDetails):
    user_id = userDetails['bungie_id']
    membership_type = userDetails['membership_type']
    access_token = userDetails['access_token']
        
    headers = {
            'X-API-Key': API_KEY,
            'Authorization': f'Bearer {access_token}'
        }
        
    weaponsList, destinyID = getWeaponsList(user_id)  # This needs to be converted to a synchronous call
        
    currentWeaponList = getCurrentWeaponsList(destinyID, membership_type, headers)  # Adapt this function to be synchronous
        
        # Log the counts
    logger.info(f"User ID: {user_id} has {len(weaponsList)} weapons in the database.")
    logger.info(f"User ID: {user_id} has {len(currentWeaponList)} weapons in the current inventory.")
        
    weaponsListSet = set(weaponsList)
    currentWeaponListSet = set(item['itemInstanceId'] for item in currentWeaponList)
        
        # Identify new weapons by finding instance IDs in currentWeaponList not in weaponsList
    new_weapons = currentWeaponListSet - weaponsListSet
        
    if new_weapons:
        logger.info(f"User ID: {user_id} has new weapons: {new_weapons}")
            
        new_weapon_responses =[]
            
        new_item_hashes = []
        for weapon in new_weapons:
            new_weapon_details = get_weapon_details(weapon, membership_type, destinyID, headers)
            weapon_hash = new_weapon_details['Response']['item']['data']['itemHash']       
            new_item_hashes.append(weapon_hash)
            new_weapon_responses.append(new_weapon_details)
                
        logging.info(f"New weapon hashes: {new_item_hashes}")
                
        weapon_details = load_weapon_names(new_item_hashes)
            
        sanitised_weapons = []
            
        for weapon in new_weapon_responses:
            associated_weapon_details = weapon_details.get(weapon['Response']['item']['data']['itemHash'], None)
            if associated_weapon_details:
                extracted_details = extract_item_details(weapon, associated_weapon_details)
                if extracted_details:
                    sanitised_weapons.append(extracted_details)      
            
        final_weapons = []
        for weapon in sanitised_weapons:
            completed_weapon = appraise_weapon(weapon, user_id, destinyID)
            final_weapons.append(completed_weapon)
            add_to_recent_weapons(completed_weapon, user_id)
            send_notification(completed_weapon['weaponName'])  
        add_weapons_to_mongodb(final_weapons, user_id)
        add_to_instance_list(final_weapons, user_id)          
    else :
        logging.info(f"No new weapons found for user ID: {user_id}")

def getWeaponsList(bungie_id):
    collection = db['UserInstanceList']
    
    # Await the asynchronous operation to get the result
    user = collection.find_one({'bungieID': bungie_id})
    
    # Now you can safely access `user['weapons']` assuming the user exists and has a 'weapons' field
    if user and 'weapons' in user:
        item_ids = [weapon['itemInstanceId'] for weapon in user['weapons']]
        return item_ids, user['destinyID']
    else:
        return []
    
def getCurrentWeaponsList(destiny_id, membership_type, headers):
    profile_data = fetch_profile_data(destiny_id, membership_type, headers)
    currentWeapons = process_weapons_from_data(profile_data, membership_type, destiny_id, headers)
    return currentWeapons

def fetch_profile_data(destiny_membership_id, destiny_membership_type, headers):
    profile_url = f"https://www.bungie.net/Platform/Destiny2/{destiny_membership_type}/Profile/{destiny_membership_id}/?components=200,102"
    response = requests.get(profile_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch profile data with status code: {response.status_code} for user ID: {destiny_membership_id}")


def process_weapons_from_data(profile_data, destiny_membership_type, destiny_membership_id, headers):
    character_ids = profile_data['Response']['characters']['data'].keys()
    vault_items = profile_data['Response']['profileInventory']['data']['items']

    all_weapons = []  # Store tuples or dictionaries of weapon hashes and instance IDs
    
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
            
    return all_weapons

def get_weapon_details(item_instance_id, destiny_membership_type, destiny_membership_id, headers):
    item_details_url = f"https://www.bungie.net/Platform/Destiny2/{destiny_membership_type}/Profile/{destiny_membership_id}/Item/{item_instance_id}/?components=300,302,304,305,307"
    response = requests.get(item_details_url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        logging.error(f"Failed to fetch item details for instance {item_instance_id}. Status code: {response.status_code}")
        return None
    
def extract_item_details(weapon, weapon_details):
    try:
        item_details = weapon['Response']['item']['data']
        sockets_details = weapon['Response']['sockets']['data']['sockets']
        instance_details = weapon['Response']['instance']['data']
        stats_details = weapon['Response']['stats']['data']
                
        item_hash = item_details.get('itemHash', 'No itemHash found')
        iteminstanceid = item_details.get('itemInstanceId', 'No itemInstanceId found')
        if sockets_details is not None:
            sockets = [sockets_details[i]['plugHash'] if i < len(sockets_details) else 'No plugHash found' for i in range(1, 5)]
        else:
            sockets = ['No plugHash found'] * 4  # Default value if sockets_details is None
        
        # Lookup weapon name and tierTypeName using item_hash
        weapon_info = weapon_details
        
        # Only include items with a valid weapon name and tierTypeName is Legendary
        if weapon_info:
            extracted_detail = {
                'itemId': iteminstanceid,
                'weaponHash': item_hash,
                'weaponName': weapon_info.get('name', 'N/A'),  # Provide 'N/A' as default value
                'icon': weapon_info.get('icon', 'N/A'),  # Provide 'N/A' as default value
                'instance': instance_details if instance_details else 'N/A',  # Provide 'N/A' as default value
                'socketHashes': sockets if sockets else [],  # Provide empty list as default value
                'socketsDetails': sockets_details if sockets_details else [],  # Provide empty list as default value
                'statDetails': stats_details if stats_details else []  # Provide empty list as default value
            }
            return extracted_detail
    except KeyError as e:
        logging.error(f"Error extracting details for weapon: {e}")
        return None
    
def load_weapon_names(weapon_hashes):
    collection = db['WeaponDetails']
    data = collection.find({"id": {"$in": weapon_hashes}})
    weapon_names_by_id = {weapon['id']: {'name': weapon['name'], 'tierTypeName': weapon['rarity'], 'icon': weapon['iconPath']} for weapon in data}
    
    return weapon_names_by_id

def appraise_weapon(weapon, bungieID, destiny_id):
    godrolls = db["GodRolls"]

    # Process the weapon
    result = process_weapon(weapon, godrolls)
    if result:
        score = result.get('score')
        if score:
            x, y = map(int, score.split('/'))
            result['score_float'] = x / y if y != 0 else 0
        else:
            result['score_float'] = 0  # Default score for weapons without a score

    # Process result as needed
    score = result.get('score', 'No score')  # Default text for weapons without a score

    completed_weapon = result

    return completed_weapon

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

def add_weapons_to_mongodb(weapons, bungieID):
    collection = db['UserInventory']
    doc = collection.find_one({'bungieID': bungieID})
    
    if doc:
        docweapons = doc.get('weapons', [])
        docweapons.append(weapons)
        
        collection.update_one({'bungieID': bungieID}, {'$set': {'weapons': docweapons}})
    
    logging.info(f"Added weapons to MongoDB for user ID: {bungieID}")

def add_to_instance_list(weapons, bungieID):
    collection = db['UserInstanceList']
    
    user = collection.find_one({'bungieID': bungieID})
    
    newWeapons = user.get('weapons', [])
        
    for weapon in weapons:
        # Extract only itemId and weaponHash from each weapon
        simplified_weapon = {'itemHash': weapon.get('weaponHash'), 'itemInstanceId': weapon.get('itemId')}
        newWeapons.append(simplified_weapon)
        logging.info(f"Added weapon to instance list: {simplified_weapon['itemInstanceId']}")
    
    collection.update_one({'bungieID': bungieID}, {'$set': {'weapons': newWeapons}})
    logging.info(f"Added {weapons.length} weapons to instance list for user ID: {bungieID}")
    
def add_to_recent_weapons(weapon, bungieID):
    collection = db['UserLatestWeapons']
    
    user = collection.find_one({'bungieID': bungieID})
    weapons = user.get('weapons', []) if user else []
    
    weapons.append(weapon)
    
    weapons_schema = {
        'bungieID': bungieID,
        'weapons': weapons
    }
    
    collection.replace_one({'bungieID': bungieID}, weapons_schema, upsert=True)
    
    logging.info(f"Added weapons to recent weapons for user ID: {bungieID}")
    
    
def send_notification(weapon_name):
    push_service = FCMNotification(api_key="AAAAGwmf8x0:APA91bFFQQNIeJLb1WkGRuHE7u8GDBaRs7D3ZqNMKRIDszBCyQ_DUuutxLURroMNxxDr2h4KCTTZxIKB0lcpjE27lQISBhT9Cfe0lGBXJl_dWsl3WJcBMqQRkkeuqsAezkpGgp_-Zj2A")

    message_title = "New Weapon Found!"
    message_body = "Found a new " + weapon_name + " in your inventory!"
    registration_id = "dBSycJ7iTt2KpagVtYahhf:APA91bEBST9UezmIHnmjOXZqQyX2MPYAZyOfCPPWVH5aI1CjbBWjgT0TyXAuQTkBYi4YyQkH9pgm_QjXPWxwkbFXnQl7PvG_g25IkxRT9cSnR7DG8P9o5txcsIv78Jg82KJfo_QiX06k"
    data_message = {"key": "value", "key2": "value2"}

    # Send a notification
    result = push_service.notify_single_device(registration_id=registration_id, message_title=message_title, message_body=message_body, data_message=data_message)

    print(result)