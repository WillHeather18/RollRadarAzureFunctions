import logging
import azure.functions as func
from pymongo import MongoClient, errors as mongo_errors
from datetime import datetime
import os
import requests

try:
    client = MongoClient(os.environ.get('MONGODB_URI'))
    db = client['RollRadar']
    collection = db['UserDetails']
    logging.info("Connected to MongoDB")
except mongo_errors.PyMongoError as e:
    logging.error(f"MongoDB connection error: {e}")
    # Consider adding a mechanism to halt or exit the function if critical connections cannot be established.

CLIENT_ID = os.environ.get('CLIENT_ID')
CLIENT_SECRET = os.environ.get('CLIENT_SECRET')
API_KEY = os.environ.get('API_KEY')
        
def refresh_all_tokens():
    try:
        users = collection.find({})
        for user in users:
            new_tokens = refresh_access_token(user['refresh_token'])
            if new_tokens:
                store_in_db(user, new_tokens['access_token'], new_tokens['refresh_token'], True)
                logging.info(f"Token refreshed for user: {user['bungie_id']}")
            else:
                logging.warning(f"Failed to refresh token for user: {user['bungie_id']}")
        logging.info("All tokens processed.")
    except Exception as e:
        logging.error(f"Error in refresh_all_tokens: {e}")
        raise
            
def refresh_access_token(refresh_token):
    try:
        url = 'https://www.bungie.net/platform/app/oauth/token/'
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded',
            'X-API-Key': API_KEY
        }
        data = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        }

        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"Failed to refresh access token, status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        logging.error(f"HTTP request error: {e}")
        return None
    
def store_in_db(user, access_token, refresh_token, refreshed):
    try:
        document = {
            'bungie_id': user['bungie_id'],
            'membership_type': user['membership_type'],
            'destiny_membership_id': user.get('destiny_membership_id'),  # Use .get() to avoid KeyError
            'display_name': user.get('display_name', 'Default Display Name'),
            'icon_url': user.get('icon_url', 'Default Icon URL'),
            'access_token': access_token,
            'refresh_token': refresh_token,
            'timestamp': datetime.now(),
            'refreshed': refreshed
        }
        collection.replace_one({'bungie_id': user['bungie_id']}, document, upsert=True)
    except mongo_errors.PyMongoError as e:
        logging.error(f"MongoDB operation error for user {user['bungie_id']}: {e}")
        raise
