import datetime
from google.cloud import bigquery

def aggregate_data(event, context):
    client = bigquery.Client()
    dataset_id = 'main'
    table_id_latest = 'sensor_data_latest'
    table_id_m = 'sensor_data_m'
    now = datetime.datetime.utcnow()
    minute_start = now.replace(second=0, microsecond=0)
    minute_end = minute_start + datetime.timedelta(minutes=1)
    delete_threshold = now - datetime.timedelta(minutes=90)
    
        # Aggregate the data
    aggregation_query = f"""
        INSERT INTO `{dataset_id}.{table_id_m}` (device_id, sensor_type, value, minute)
        SELECT
            device_id,
            sensor_type,
            value,
            TIMESTAMP_TRUNC(timestamp, MINUTE) as minute_timestamp
        FROM (
            SELECT
                device_id,
                sensor_type,
                value,
                timestamp,
                ROW_NUMBER() OVER (PARTITION BY device_id, sensor_type, TIMESTAMP_TRUNC(timestamp, MINUTE) ORDER BY timestamp DESC) AS rn
            FROM `{dataset_id}.{table_id_latest}`
            WHERE
                timestamp >= '{minute_start.isoformat("T")}Z' AND
                timestamp < '{minute_end.isoformat("T")}Z'
        ) WHERE rn = 1
    """

    deletion_query = f"""
        DELETE FROM `{dataset_id}.{table_id_latest}` t1
        WHERE timestamp < '{delete_threshold.isoformat("T")}Z' 
            
    """

     # Run the aggregation query
    aggregation_job = client.query(aggregation_query)
    aggregation_result = aggregation_job.result()
    rows_affected_agg = aggregation_job.num_dml_affected_rows

    # Run the deletion query
    deletion_job = client.query(deletion_query)
    deletion_result = deletion_job.result()
    rows_affected_del = deletion_job.num_dml_affected_rows

    # Check for errors in the jobs
    if aggregation_job.errors or deletion_job.errors:
        return {
            'success': False,
            'errors': {
                'aggregation': aggregation_job.errors,
                'deletion': deletion_job.errors
            }
        }
    else:
        return {
            'success': True,
            'rows_affected': {
                'aggregation': rows_affected_agg,
                'deletion': rows_affected_del
            }
        }
        