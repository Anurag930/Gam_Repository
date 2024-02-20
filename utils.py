import slack
import os
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime

# token="xoxb-6583633580359-6626357517025-ltN5q4FHIaVq5o4bQleX56y7"
# For success 
def slack_notify():
    env_path=Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    client=slack.WebClient(os.environ['SLACK_BOT_TOKEN'])
    client.chat_postMessage(channel="gam_success_notification",text=" GAM Data is extracted,standardized and loaded into snowflake and validated successfully")

# For failure
def slack_notify_alert(error_msg):
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    client = slack.WebClient(token=os.environ['SLACK_BOT_TOKEN'])
    client.chat_postMessage(channel="gam_alert_notification", text=f"Data loading failed with error: {error_msg}")

def slack_notify_empty_data():
    env_path = Path('.') / '.env'
    load_dotenv(dotenv_path=env_path)
    client = slack.WebClient(token=os.environ['SLACK_BOT_TOKEN'])
    client.chat_postMessage(channel="gam_success_notification", text="The DataFrame is empty.")

# def get_time():
#     return datetime.now().strftime('%Y%m%d%H%M%S')

def modified():
    timestamp=datetime.now().strftime('%Y%m%d%H%M')
    file_path = f'C:/Users/anurag.sharma/Documents/test_python_scripts/GAM/gam_files/GAM_{timestamp}.json'
    return file_path



def create_folder_structure(base_dir):
    now = datetime.now()
    year = str(now.year)
    month = str(now.month).zfill(2)  # Zero-padding month
    day = str(now.day).zfill(2)      # Zero-padding day
    hour = str(now.hour).zfill(2)    # Zero-padding hour
    minute = str(now.minute).zfill(2)    # Zero-padding minute
 
    folder_path = os.path.join(base_dir, year, month, day, hour, minute)
 
    
    os.makedirs(folder_path, exist_ok=True)

    return folder_path
    


    
    




