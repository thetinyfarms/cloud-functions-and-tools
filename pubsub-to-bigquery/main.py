import base64
import json
from google.cloud import bigquery


bigquery_client = bigquery.Client()

def sensor_data_latest(data, context):
    if 'data' in data:
        message = base64.b64decode(data['data']).decode('utf-8')
        message_json = json.loads(message)

        device_id = message_json['device_id']
        sensor_type = message_json['sensor_type']
        value = message_json['value']
        timestamp = message_json['timestamp']

        dataset_id = 'main'
        table_id = 'sensor_data_latest'
        table_ref = bigquery_client.dataset(dataset_id).table(table_id)
        table = bigquery_client.get_table(table_ref)

        rows_to_insert = [
            {u'device_id': device_id, u'sensor_type': sensor_type, u'value': value, u'timestamp': timestamp}
        ]

        errors = bigquery_client.insert_rows(table, rows_to_insert)

        if errors:
            print(f'Error inserting rows: {errors}')
        else:
            print(f'Successfully inserted {len(rows_to_insert)} row(s) into {dataset_id}.{table_id}')
    else:
        print('No data in message')