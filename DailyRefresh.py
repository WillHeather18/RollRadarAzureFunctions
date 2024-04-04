import logging
import datetime
from GetManifest import download_destiny_manifest
from GetAllWeapons import GetWeapons
from GetAllPerks import GetPerks
import os
import json
from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)


API_KEY = os.environ["API_KEY"]
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
MONGODB_URI = os.environ["MONGODB_URI"]
STORAGE_CONNECTION_STRING = os.environ['AzureWebJobsStorage']  # Azure Storage connection string
QUEUE_NAME = 'godrollqueue'  # Azure Queue name

def DailyRefreshCore(db) -> None:
    logging.info('RollRadarDailyRefresh function started at %s', datetime.datetime.now())
        
    starttime = datetime.datetime.now()
    
    tempfile_path = download_destiny_manifest(API_KEY) 
    
    GetWeapons(tempfile_path, db)
    
    GetPerks(tempfile_path, db)
    
    if os.path.isfile(tempfile_path):
        os.remove(tempfile_path)
        logging.info(f"Deleted temporary file")
    
    logging.info("Starting Scraping...")
    
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
        
    endtime = datetime.datetime.now()

    logging.info('RollRadarDailyRefresh function executed at %s', datetime.datetime.now())