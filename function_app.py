import logging
import azure.functions as func
from InventoryScanner import ProcessQueueMessage
from OAuthRefresh import refresh_all_tokens
from DailyRefresh import DailyRefreshCore
from Scraper import ScrapeGodRolls
import os
from datetime import datetime
from pymongo import MongoClient
from azure.storage.queue import (
        QueueClient,
        BinaryBase64EncodePolicy,
        BinaryBase64DecodePolicy
)
import json


MONGODB_URI = os.environ['MONGODB_URI']  # MongoDB connection string
DB_NAME = 'RollRadar'  # MongoDB database name
STORAGE_CONNECTION_STRING = os.environ['AzureWebJobsStorage']  # Azure Storage connection string
QUEUE_NAME = 'userinvcheck'  # Azure Queue name

clent = MongoClient(MONGODB_URI)
db = clent[DB_NAME]

app = func.FunctionApp()

@app.schedule(schedule="0 */5 * * * *", arg_name="enqueueUserChecks", run_on_startup=False,
              use_monitor=True) 
def userQueueTimer(enqueueUserChecks: func.TimerRequest) -> None:
    if enqueueUserChecks.past_due:
        logging.info('The timer is past due!')
        
    queue_client = QueueClient.from_connection_string(STORAGE_CONNECTION_STRING, QUEUE_NAME)

    queue_client.message_encode_policy = BinaryBase64EncodePolicy()
    queue_client.message_decode_policy = BinaryBase64DecodePolicy()
        
    collection = db['UserDetails']
        
    cursor = collection.find({}, {'bungie_id': 1, 'membership_type': 1, "access_token": 1})
    user_data = [{'bungie_id': doc['bungie_id'], 'membership_type': doc['membership_type'], 'access_token' : doc['access_token']} for doc in cursor]

    logging.info(user_data)

    # Enqueue each user ID and membership type
    for user in user_data:
        message = json.dumps(user)        
        logging.info(f"Enqueueing user data: {message}")

        message_bytes =  message.encode('utf-8')
        
        queue_client.send_message(
        queue_client.message_encode_policy.encode(content=message_bytes)
        )

    logging.info(f"Enqueued {len(user_data)} user checks.")

    logging.info('Python timer trigger function executed.')
    


@app.queue_trigger(arg_name="azqueue", queue_name="userinvcheck", connection="AzureWebJobsStorage")
def readQueueInventoryScanner(azqueue: func.QueueMessage):
    try:
        message_content = azqueue.get_body().decode('utf-8')
        logging.info(f"Processing User message")
        userDetails = json.loads(message_content)
        ProcessQueueMessage(userDetails)
    except Exception as e:
        logging.error(f"Error processing message: {e}")
        raise e
    


@app.schedule(schedule="0 0 * * * *", arg_name="OAuthRefresh", run_on_startup=False, use_monitor=True)
def OAuth_timer(OAuthRefresh: func.TimerRequest) -> None:
    if OAuthRefresh.past_due:
        logging.info('The timer is past due!')
    logging.info('Python 1hr OAuth Refresh trigger function ran at %s', datetime.now().isoformat())
    try:
        refresh_all_tokens()
    except Exception as e:
        logging.error(f"Error refreshing tokens: {e}")
    
    logging.info('Python timer trigger function executed.')  
    


@app.function_name("RollRadarDailyRefresh")
@app.schedule(schedule="0 0 5 * * *", arg_name="dailyRefresh", run_on_startup=True, use_monitor=True)
def RollRadarDailyRefresh(dailyRefresh: func.TimerRequest) -> None:
    if dailyRefresh.past_due:
        logging.info('The timer is past due!')
        
    logging.info('Python daily refresh trigger function ran at %s', datetime.now().isoformat())
    
    try:
        DailyRefreshCore()
    except Exception as e:
        logging.error(f"Error running daily refresh: {e}")
        
        
@app.queue_trigger(arg_name="godrollscraper", queue_name="godrollqueue", connection="AzureWebJobsStorage")
def ScrapeGodRoll(godrollscraper: func.QueueMessage):
    try:        
        message_content = godrollscraper.get_body().decode('utf-8')
        queuetime = json.loads(message_content)
        
        ScrapeGodRolls(db)
        
        logging.info("Scraped God Rolls")
    except Exception as e:
        logging.error(f"Error scraping godrolls: {e}")
        

@app.queue_trigger(arg_name="dailyinvcheck", queue_name="dailyinvqueue", connection="AzureWebJobsStorage")
def DailyInventoryCheck(dailyinvcheck: func.QueueMessage):
    try:
        message_content = dailyinvcheck.get_body().decode('utf-8')
        queuetime = json.loads(message_content)
        logging.info(f"Processing invcheck message")

    except Exception as e:
        logging.error(f"Error processing message: {e}")
        raise e
    
@app.queue_trigger(arg_name="dailyinvuser", queue_name="dailyinvusers", connection="AzureWebJobsStorage")
def DailyInventoryUser(dailyinvuser: func.QueueMessage):
    try:
        message_content = dailyinvuser.get_body().decode('utf-8')
        userDetails = json.loads(message_content)
        logging.info(f"Processing invcheck message")

    except Exception as e:
        logging.error(f"Error processing message: {e}")
        raise e


