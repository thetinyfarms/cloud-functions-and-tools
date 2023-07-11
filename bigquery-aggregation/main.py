import logging
from flask import abort, jsonify
import datetime
from google.cloud import bigquery

DURATIONS = ["1m", "15m", "90m", "8h"]

def aggregate_data(request, context=None):
    #if request.method != 'POST':
    #    return abort(405)
    duration = request.values.get('duration')
    if duration not in DURATIONS:
        return abort(400)
    
    client = bigquery.Client()
    assert client.project == 'tinyfarms-website'
    now = datetime.datetime.utcnow()
    match duration:
        case "1m" | "15m":
            start = now.replace(second=0, microsecond=0)
            end = start + datetime.timedelta(minutes=int(duration[:-1])) # gets either 1 or 15
            trunc = 
        case "90m":
            start = now.replace(hour=now.hour-1, minute=now.minute-30, second=0, microsecond=0)
            end = start + datetime.timedelta(minutes=90)
        case "8h":
            start = now.replace(hour=now.hour-8, minute=0, second=0, microsecond=0)
            end = start + datetime.timedelta(hours=8)
        case _:
            raise ValueError(f"Invalid duration: {duration}")

    now.replace(second=0, microsecond=0)
    delete_threshold = now - datetime.timedelta(minutes=90)
    
    # Aggregate the data
    aggregation_query = f"""
        INSERT INTO `main.sensor_data_{duration}` (device_id, sensor_type, value, timestamp)
        SELECT
            device_id,
            sensor_type,
            value,
            TIMESTAMP_TRUNC(timestamp, MINUTE) as {duration}_timestamp
        FROM (
            SELECT
                device_id,
                sensor_type,
                value,
                timestamp,
                ROW_NUMBER() OVER (PARTITION BY device_id, sensor_type, TIMESTAMP_TRUNC(timestamp, MINUTE) ORDER BY timestamp DESC) AS rn
            FROM `main.sensor_data_latest`
            WHERE
                timestamp >= '{start.isoformat("T")}Z' AND
                timestamp < '{end.isoformat("T")}Z'
        ) WHERE rn = 1
    """

    deletion_query = f"""
        DELETE FROM `main.sensor_data_latest`
        WHERE timestamp < '{delete_threshold.isoformat("T")}Z' 
            
    """

     # Run the aggregation query
    aggregation_job = client.query(aggregation_query)
    aggregation_result = aggregation_job.result()
    assert aggregation_job.state == 'DONE'
    rows_affected_agg = aggregation_job.num_dml_affected_rows
    assert type(rows_affected_agg) == int

    # Run the deletion query
    deletion_job = client.query(deletion_query)
    deletion_result = deletion_job.result()
    assert deletion_job.state == 'DONE'
    rows_affected_del = deletion_job.num_dml_affected_rows
    assert type(rows_affected_del) == int

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
        response_data = {
        'success': True,
        'rows_affected': {
            'aggregation': rows_affected_agg,
            'deletion': rows_affected_del,
            'minute_start': minute_start.isoformat("T")+'Z',
            'minute_end': minute_end.isoformat("T")+'Z'
        }
        }
        logging.warning(f"Response data: {response_data}")  
        return jsonify(response_data), 200

aggregate_data(None)