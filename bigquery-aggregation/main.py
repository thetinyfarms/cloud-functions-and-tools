import logging
from flask import abort, jsonify
import datetime
from google.cloud import bigquery

DURATIONS = ["1m", "15m", "90m", "8h"]
SCHEMAS = ["latest","m", "15m", "90m", "8h"]


def aggregate_data(request, context=None):
    #if request.method != 'POST':
    #    return abort(405)
    duration = "15m"#request.values.get('duration')
    if duration not in DURATIONS:
        return abort(400)
    
    client = bigquery.Client()
    assert client.project == 'tinyfarms-website'
    now = datetime.datetime.utcnow()
    end = now.replace(second=0, microsecond=0)
    match duration:
        case "1m" | "15m":
            start = end - datetime.timedelta(minutes=int(duration[:-1])) # gets either 1 or 15
        case "90m":
            start = end - datetime.timedelta(minutes=90)
        case "8h":
            start = end - datetime.timedelta(hours=8)
        case _:
            raise ValueError(f"Invalid duration: {duration}")
        
    duration_before = SCHEMAS[DURATIONS.index(duration)]
    time_col = "timestamp" if duration_before == "latest" else "minute"
    time_scale = "MINUTE" if 'm' in duration else "HOUR"
    truncate = "HOUR" if 'm' in duration else "DAY"
    interval = int(duration[:-1])
    now.replace(second=0, microsecond=0)
    delete_threshold = now - datetime.timedelta(minutes=90)
    
    # Aggregate the data
    aggregation_query = f"""
        INSERT INTO `main.sensor_data_{duration}` (device_id, sensor_type, value, minute)
        SELECT
            device_id,
            sensor_type,
            value,
            TIMESTAMP_ADD(TIMESTAMP_TRUNC({time_col}, {truncate}), INTERVAL CAST(ROUND(EXTRACT({time_scale} FROM {time_col}) / {float(interval)}) * {interval} AS INTEGER) {time_scale}) as minute
        FROM (
            SELECT
                device_id,
                sensor_type,
                value,
                {time_col},
                ROW_NUMBER() OVER (PARTITION BY device_id, sensor_type, 
                TIMESTAMP_ADD(TIMESTAMP_TRUNC({time_col}, {truncate}), INTERVAL CAST(ROUND(EXTRACT({time_scale} FROM {time_col}) / {float(interval)}) * {interval} AS INTEGER) {time_scale})
                ORDER BY {time_col} DESC) AS rn
            FROM `main.sensor_data_{duration_before}`
            WHERE
                {time_col} >= '{start.isoformat("T")}Z' AND
                {time_col} < '{end.isoformat("T")}Z'
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

    deletion_job = client.query(deletion_query)
    if duration == "8h":
        # Run the deletion query
        deletion_result = deletion_job.result()
        assert deletion_job.state == 'DONE'

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
            'deletion':  deletion_job.num_dml_affected_rows or 0 ,
            'start': start.isoformat("T")+'Z',
            'end': end.isoformat("T")+'Z'
        }
        }
        logging.warning(f"Response data: {response_data}")  
        return jsonify(response_data), 200

aggregate_data(None)