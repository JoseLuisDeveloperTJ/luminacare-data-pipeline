import requests
import boto3
import json
from datetime import datetime 
from dotenv import load_dotenv
import os

load_dotenv()

# SETUP
MOCKAROO_URL = "https://my.api.mockaroo.com/lumina_tickets?key=178108a0"
BUCKET_NAME = "luminacare-datalake-luis"

def test_upload():
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Content-Type": "application/json"
    }

    print("🚀 Attempting to fetch data from Mockaroo...")

    try:
        response = requests.get(MOCKAROO_URL, headers=headers, timeout=15)
        
        if response.status_code == 200:
            data = response.json()

            if isinstance(data, dict) and "error" in data:
                print(f"❌ API Internal Error: {data['error']}")
                return

            print(f"✅ Data received successfully ({len(data)} records).")

            now = datetime.now()
            file_name = f"raw/year={now.year}/month={now.month}/day={now.day}/tickets_{now.strftime('%H%M%S')}.json"

            s3 = boto3.client(
                's3',
                aws_access_key_id=os.getenv("ACCESS_KEY"),
                aws_secret_access_key=os.getenv("SECRET_KEY")
            )
            
            # 5. Upload to S3 Bucket
            s3.put_object(
                Bucket=BUCKET_NAME,
                Key=file_name,
                Body=json.dumps(data, indent=4) 
            )
            print(f"📤 Successfully uploaded to: s3://{BUCKET_NAME}/{file_name}")
        
        else:
            print(f"❌ Request Failed. Status Code: {response.status_code}")
            print(f"Server Response: {response.text}")

    except requests.exceptions.RequestException as e:
        print(f"💥 Network error while connecting to Mockaroo: {e}")
    except Exception as e:
        print(f"💥 An unexpected error occurred: {e}")

if __name__ == "__main__":
    test_upload()