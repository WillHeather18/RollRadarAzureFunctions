import json
import logging
import os
from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)


STORAGE_CONNECTION_STRING = os.environ['AzureWebJobsStorage']  # Azure Storage connection string
QUEUE_NAME = 'godrollqueue'  # Azure Queue name

def enqueue_weapons(urls_and_names):
    queue_client = QueueClient.from_connection_string(STORAGE_CONNECTION_STRING, QUEUE_NAME)
    
    queue_client.message_encode_policy = BinaryBase64EncodePolicy()
    queue_client.message_decode_policy = BinaryBase64DecodePolicy()
    
    for weapon in urls_and_names:
        
        message = json.dumps(weapon)
        
        message_bytes =  message.encode('utf-8')
        
        queue_client.send_message(
        queue_client.message_encode_policy.encode(content=message_bytes)
        )
    
    logging.info(f"Enqueued {len(urls_and_names)} weapons for scraping.")

def generate_and_enqueue_urls(db):
    
    collection = db["WeaponDetails"]

    urls_and_names = []
    for weapon in collection.find():
        name_for_url = weapon['name'].replace(' ', '-').lower()
        url = f"https://www.light.gg/db/items/{weapon['id']}/{name_for_url}/"
        urls_and_names.append({'url': url, 'name': weapon['name'], 'id': weapon['id']})
    
    enqueue_weapons(urls_and_names)

    logging.info(f"Generated {len(urls_and_names)} URLs for scraping weapon details.")
