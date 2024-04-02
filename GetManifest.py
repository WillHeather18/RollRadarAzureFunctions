import requests
import io
import zipfile
import logging
import tempfile

def upload_progress_callback(current, total):
    print(f"Uploaded {current} of {total} bytes ({(current/total)*100:.2f}%)")


def download_destiny_manifest(api_key):
    
    azure_logger = logging.getLogger('azure')
    azure_logger.setLevel(logging.WARNING)
    
    manifest_url = "https://www.bungie.net/Platform/Destiny2/Manifest/"
    container_name = "rollradar-functions"  # Replace with your actual container name
    headers = {"X-API-Key": api_key}
    
    response = requests.get(manifest_url, headers=headers)
    if response.status_code == 200:
        manifest_data = response.json()
        manifest_path = manifest_data['Response']['mobileWorldContentPaths']['en']
        full_manifest_url = f"https://www.bungie.net{manifest_path}"
        
        manifest_response = requests.get(full_manifest_url, stream=True)
        manifest_content = manifest_response.content
        
        logging.info(f"Downloaded manifest from: {full_manifest_url}")

        with zipfile.ZipFile(io.BytesIO(manifest_content), 'r') as zip_ref:
            manifest_names = [name for name in zip_ref.namelist() if name.endswith('.content')]
            if manifest_names:
                manifest_name = manifest_names[0]
                with zip_ref.open(manifest_name) as manifest_file, tempfile.NamedTemporaryFile(delete=False) as temp_file:
                    manifest_data = manifest_file.read()
                    temp_file.write(manifest_data)
                    # Now temp_file.name contains the path to the temporary file where the manifest is stored.
                    logging.info(f"Stored manifest temporarily at: {temp_file.name}")
                    
                    # You can now upload the manifest data from temp_file or use it directly
                    manifest_name = manifest_name.replace('.content', '.sqlite3')
                    logging.info(f"Extracted and uploaded manifest: {manifest_name}")
                    return temp_file.name
            else:
                logging.error("No manifest file found in the ZIP.")
    else:
        logging.error("Failed to get the manifest location.")

# Replace with your actual values
