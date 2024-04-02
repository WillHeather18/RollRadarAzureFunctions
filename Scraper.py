import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import datetime
import json
import os
from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)

STORAGE_CONNECTION_STRING = os.environ['AzureWebJobsStorage']  # Azure Storage connection string
QUEUE_NAME = 'dailyinvqueue'  # Azure Queue name


def generate_urls(db):
    collection = db["WeaponDetails"]

    urls_and_names = []
    for weapon in collection.find():
        name_for_url = weapon['name'].replace(' ', '-').lower()
        url = f"https://www.light.gg/db/items/{weapon['id']}/{name_for_url}/"
        urls_and_names.append({'url': url, 'name': weapon['name'], 'id': weapon['id']})
    
    logging.info(f"Generated {len(urls_and_names)} URLs for scraping weapon details.")

    return urls_and_names

def fetch_weapon_details(weapon, session):
    try:
        response = session.get(weapon['url'])
        if not response.ok:  # response.ok is True for HTTP status codes 2xx
            print(f"Failed to fetch {weapon['url']}: {response.status_code}")
        soup = BeautifulSoup(response.text, 'html.parser')
        community_average_div = soup.find('div', id='community-average')
        weapon_details = {'weaponHash': weapon['id'], 'sockets_details': []}

        all_perk_names = []

        if community_average_div:
            containers = community_average_div.find_all('ul', class_='list-unstyled sockets')
            for container in containers:
                socket_details = []
                list_items = container.find_all('li')
                for item in list_items:
                    percent_div = item.find('div', class_='percent')
                    percentage = percent_div.text.strip() if percent_div else None

                    image = item.find('img')
                    alt_text = image.get('alt') if image else None
                    
                    if alt_text: all_perk_names.append(alt_text)

                    item_detail = {
                        'percentage': percentage,
                        'name': alt_text,
                    }
                    socket_details.append(item_detail)
                
                if socket_details:
                    weapon_details['sockets_details'].append(socket_details)
        
        return (weapon_details, all_perk_names)
    except Exception as e:
        logging.error(f"Error fetching details for {weapon['url']}: {e}")
        return None, []

def find_hashes_by_names(db, all_perk_names):
    collection = db['PerkDetails']
    perk_names_set = set(all_perk_names)  # Convert list to set for O(1) lookups
    found_hashes = {}

    # Fetch all perks that could potentially be in all_perk_names to minimize DB reads
    try:
        query = {"displayProperties.name": {"$in": list(perk_names_set)}, "inventory.tierType": 2}
        records = collection.find(query, {"displayProperties.name": 1, "hash": 1})
        for item in records:
            name = item.get("displayProperties", {}).get("name")
            if name:
                found_hashes[name] = item['hash']
    except Exception as e:
        logging.error(f"An error occurred while reading perk hashes: {e}")

    return found_hashes

def scrape_details_and_save(urls_and_names, db):
    collection = db["GodRolls"]
    session = requests.Session()
    weapon_details_list = []

    logging.info(f"Scraping details for {len(urls_and_names)} weapons.")

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_weapon = {executor.submit(fetch_weapon_details, weapon, session): weapon for weapon in urls_and_names}
        counter = 0
        all_perk_names = []  # Collect all perk names first to minimize database queries
        for future in as_completed(future_to_weapon):
            logging.info(f"Scraping weapon details:{weapon_details}")
            weapon_details, weapon_perk_names = future.result()
            if weapon_details and weapon_details['sockets_details']:
                all_perk_names.extend(weapon_perk_names)  # Append perks for later processing
                weapon_details_list.append(weapon_details)
            counter += 1
            if counter % 100 == 0:
                logging.info(f"Scraped details for {counter} weapons.")

    # Find perk hashes after collecting all perk names to reduce DB queries
    perk_hashes = find_hashes_by_names(db, all_perk_names)
    for weapon_details in weapon_details_list:
        for socket in weapon_details['sockets_details']:
            for item in socket:
                if item['name'] in perk_hashes:
                    item['socketHash'] = perk_hashes[item['name']]

    # Database operations
    if weapon_details_list:
        collection.delete_many({})
        collection.insert_many(weapon_details_list)
    logging.info(f"Inserted {len(weapon_details_list)} weapons into MongoDB.")

# Script execution
def ScrapeGodRolls(db):

    urls_and_names = generate_urls(db)
    scrape_details_and_save(urls_and_names, db)
    logging.info("Scraping completed and details saved to MongoDB collection.")
    
    queue_client = QueueClient.from_connection_string(STORAGE_CONNECTION_STRING, QUEUE_NAME)
    
    queue_client.message_encode_policy = BinaryBase64EncodePolicy()
    queue_client.message_decode_policy = BinaryBase64DecodePolicy()
    
    

    message = {
               "timestamp" : datetime.datetime.now().isoformat(),
               }
    
    message_json = json.dumps(message)
       
    message_bytes =  message_json.encode('utf-8')
    
    queue_client.send_message(
    queue_client.message_encode_policy.encode(content=message_bytes)
    )


