import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed
import logging
import datetime
import json
import time
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
    
    logging.info(f"ScrapingLOG: Generated {len(urls_and_names)} URLs for scraping weapon details.")

    return urls_and_names

def fetch_weapon_details(weapon, session):
    # Set a User-Agent header to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    try:
        # Use the session with headers for the request
        response = session.get(weapon['url'], headers=headers)
        if not response.ok:  # response.ok is True for HTTP status codes 2xx
            print(f"ScrapingLOG: Failed to fetch {weapon['url']}: {response.status_code}")
            return None, []

        soup = BeautifulSoup(response.text, 'html.parser')
        community_average_div = soup.find('div', id='community-average')
        masterwork_div = soup.find('div', id='masterwork-stats')
        mods_div = soup.find('div', id='mod-stats')
        weapon_details = {'weaponHash': weapon['id'], 'sockets_details': [], 'masterworks': [], 'mods': []}

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
                    
        if masterwork_div:
            masterwork_blocks = masterwork_div.find_all('div', class_='clearfix')
            for block in masterwork_blocks:
                perk_container = block.find('div', class_='perk-container')
                if perk_container:
                    item = perk_container.find('div', class_='item')
                    if item:
                        data_id = item.get('data-id')
                        img_tag = item.find('img')
                        image_url = img_tag.get('src') if img_tag else None
                        alt_text = img_tag.get('alt') if img_tag else None

                        # Getting percentage
                        percent_div = block.find('div', class_='combo-percent')
                        percentage = percent_div.text.strip() if percent_div else None

                        masterwork_details = {
                            'id': data_id,
                            'name': alt_text,
                            'image_url': image_url,
                            'percentage': percentage
                        }
                        weapon_details['masterworks'].append(masterwork_details)
                        
        if mods_div:
            mod_blocks = mods_div.find_all('div', class_='clearfix')
            for block in mod_blocks:
                perk_container = block.find('div', class_='perk-container')
                if perk_container:
                    item = perk_container.find('div', class_='item')
                    if item:
                        data_id = item.get('data-id')
                        img_tag = item.find('img')
                        image_url = img_tag.get('src') if img_tag else None
                        alt_text = img_tag.get('alt') if img_tag else None

                        # Getting percentage
                        percent_div = block.find('div', class_='combo-percent')
                        percentage = percent_div.text.strip() if percent_div else None

                        mod_details = {
                            'id': data_id,
                            'name': alt_text,
                            'image_url': image_url,
                            'percentage': percentage
                        }
                        weapon_details['mods'].append(mod_details)

        # Delay to avoid hitting the server too frequently
        time.sleep(1)

        return (weapon_details, all_perk_names)
    except Exception as e:
        print(f"ScrapingLOG: Error fetching details for {weapon['url']}: {e}")
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
        logging.error(f"ScrapingLOG: An error occurred while reading perk hashes: {e}")

    return found_hashes

def scrape_details_and_save(urls_and_names, db):
    collection = db["GodRolls"]
    session = requests.Session()
    weapon_details_list = []

    logging.info(f"ScrapingLOG: Scraping details for {len(urls_and_names)} weapons.")

    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_weapon = {executor.submit(fetch_weapon_details, weapon, session): weapon for weapon in urls_and_names}
        counter = 0
        all_perk_names = []  # Collect all perk names first to minimize database queries
        for future in as_completed(future_to_weapon):
            weapon_details, weapon_perk_names = future.result()
            if weapon_details and weapon_details['sockets_details']:
                all_perk_names.extend(weapon_perk_names)  # Append perks for later processing
                weapon_details_list.append(weapon_details)
            counter += 1
            if counter % 100 == 0:
                logging.info(f"ScrapingLOG: Scraped details for {counter} weapons.")

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
    logging.info(f"ScrapingLOG: Inserted {len(weapon_details_list)} weapons into MongoDB.")
    
def getGodRollOverview():
    url = 'https://www.light.gg/'  # URL of Light.gg
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }

    response = requests.get(url, headers=headers)
    if response.ok:
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Define the CSS selectors for the elements we're interested in
        selectors = [
            "#trending-items > div > div:nth-child(2) > div:nth-child(3) > div:nth-child(1) > a",
            "#trending-items > div > div:nth-child(2) > div:nth-child(3) > div:nth-child(2) > a",
            '#trending-items > div > div:nth-child(2) > div:nth-child(3) > div:nth-child(3) > a',
            '#trending-items > div > div:nth-child(2) > div:nth-child(3) > div:nth-child(4) > a',
            '#trending-items > div > div:nth-child(2) > div:nth-child(3) > div:nth-child(5) > a',
            '#trending-items > div > div:nth-child(2) > div:nth-child(3) > div:nth-child(6) > a',
        ]

        god_roll_ids = []
        for selector in selectors:
            element = soup.select_one(selector)
            if element:
                data_id = element.get('data-id')
                if data_id:
                    god_roll_ids.append(data_id)
                    print(f"Found item with data-id: {data_id}")

        # Here you can further process the god_roll_ids or save them to the database
        return god_roll_ids
    else:
        print(f"Failed to fetch {url}: {response.status_code}")
        return []
    
def StorePopularIds(db, popular_ids):
    collection = db["PopularGodRolls"]
    collection.delete_many({})
    collection.insert_one({"popular_ids": popular_ids})
    logging.info(f"ScrapingLOG: Stored {len(popular_ids)} popular God Rolls in MongoDB.")

def ScrapeGodRolls(db):
    start_time = datetime.datetime.now()  # Record the start time
    
    popular_ids = getGodRollOverview()
    StorePopularIds(db, popular_ids)

    urls_and_names = generate_urls(db)
    scrape_details_and_save(urls_and_names, db)

    end_time = datetime.datetime.now()  # Record the end time
    duration = end_time - start_time  # Calculate the duration

    logging.info(f"ScrapingLOG: Scraping completed and details saved to MongoDB collection. Time taken: {duration}")

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


