import base64
import json
from google.cloud import storage
from PIL import Image
import io

storage_client = storage.Client()

def save_image_data(data, context):
    if 'data' in data:
        message = base64.b64decode(data['data']).decode('utf-8')
        message_json = json.loads(message)

        device_id = message_json['device_id']
        sensor_type = message_json['sensor_type']
        value = message_json['value']
        timestamp = message_json['timestamp']

        if sensor_type != 'camera':
            print('Not a camera image, ignoring')
            return

        bucket_name = f'{device_id}_images'
        bucket = storage_client.lookup_bucket(bucket_name)
        
        if bucket is None:
            print(f'Bucket {bucket_name} does not exist, creating one')
            bucket = storage_client.create_bucket(bucket_name)
            print(f'Bucket {bucket_name} created')
            
        # Decode the base64 image data
        image_data = base64.b64decode(value)

        # Use PIL to determine the format of the image
        image = Image.open(io.BytesIO(image_data))
        format = image.format.lower()

        # Create a blob with the name based on the timestamp and the format
        blob = bucket.blob(str(timestamp) + '.' + format)
        blob.upload_from_string(image_data)

        print(f'Successfully saved image in {bucket_name}/{timestamp}.{format}')
    else:
        print('No data in message')
