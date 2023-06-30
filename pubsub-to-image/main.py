import base64
import json
from google.cloud import storage
from PIL import Image
import io
from datetime import datetime

storage_client = storage.Client()

def save_image_data(event, context):
    # Access the base64 data directly from the 'data' attribute in the Pub/Sub message
    message = base64.b64decode(event['data']).decode('utf-8')

    # The deviceId is retrieved from the 'attributes' in the Pub/Sub message
    device_id = event['attributes']['deviceId']

    bucket_name = 'tinyfarms-images' # Define your base bucket name here
    bucket = storage_client.lookup_bucket(bucket_name)
        
    if bucket is None:
        print(f'Bucket {bucket_name} does not exist, creating one')
        bucket = storage_client.create_bucket(bucket_name)
        print(f'Bucket {bucket_name} created')

    # Create a folder within the bucket for each device
    folder_name = f'{bucket_name}/{device_id}'
            
    # Decode the base64 image data
    image_data = base64.b64decode(message)

    # Use PIL to determine the format of the image
    image = Image.open(io.BytesIO(image_data))
    format = image.format.lower()

    # Generate the timestamp
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # Create a blob with the name based on the timestamp and the format
    # It's stored in the folder corresponding to the device_id
    blob = bucket.blob(f'{folder_name}/{timestamp}.{format}')
    blob.upload_from_string(image_data)

    print(f'Successfully saved image in {folder_name}/{timestamp}.{format}')
