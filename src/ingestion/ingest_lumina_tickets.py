import requests
import boto3
import json
from datetime import datetime 

load_dotenv()

def test_upload():
    print("Getting date from Mockaroo....")
    response = requests.get(MOCKAROO_URL)
    data = response.json()

    now = datetime.now()
    file_name = f"raw/year={now.year}/month={now.month}/day={now.day}/tickets_{now.strftime('%H%M%S')}.json"

    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )
    
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=file_name,
        Body=json.dumps(data)
    )
    print(f"The file has been upload succesfully to: s3://{BUCKET_NAME}/{file_name}")

if __name__ == "__main__":
    test_upload()
   