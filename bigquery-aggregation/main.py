import logging
from flask import abort, jsonify, request
import datetime
from google.cloud import bigquery

# The durations we aggregate data for
DURATIONS = ["1m", "15m", "90m", "480m"]
# Threshold is how many minutes we keep the data for
THRESHOLD = [1.5*60, 60*24*7, 60*24*35, 60*24*80] 
# The schemas we aggregate data from
SCHEMAS = ["latest","1m", "15m", "90m", "8h"]

def aggregate_data(context=None):
    if request.method != 'POST':
        return abort(405)
    # Get duration from post request
    request_json = request.get_json()
    if not request_json:
        return abort(400)
    duration = request_json.get('duration')
        
    if duration not in DURATIONS:
        return abort(400)
    interval = int(duration[:-1])
    
    client = bigquery.Client()
    assert client.project == 'tinyfarms-website'
    
    now = datetime.datetime.utcnow()
    end = now.replace(second=0, microsecond=0)
    start = end - datetime.timedelta(minutes=interval) # gets either 1 or 15
        
    duration_index = DURATIONS.index(duration)
    duration_before = SCHEMAS[duration_index]
    duration = SCHEMAS[duration_index+1]
    value_col = "average" if duration_before != "latest" else "value"

    # Aggregate the data
    aggregation_query = f"""
        INSERT INTO `main.sensor_data_{duration}` (device_id, sensor_type, average, minimum, maximum, timestamp)
        SELECT *
        FROM (
            SELECT
                device_id,
                sensor_type,
                round(avg({value_col}), 2) as average,
                min({value_col}) as minimum,
                max({value_col}) as maximum,
                TIMESTAMP_ADD(
                    TIMESTAMP_TRUNC(timestamp, DAY), 
                    INTERVAL 
                    CAST(
                        ROUND(
                            EXTRACT(HOUR FROM timestamp) * 60
                            +
                            EXTRACT(MINUTE FROM timestamp)
                        ) / {float(interval)} 
                        AS INTEGER
                    ) * {interval}
                    MINUTE
                ) as timestamp
            FROM `main.sensor_data_{duration_before}`
            WHERE
                timestamp >= '{start.isoformat("T")}Z' AND
                timestamp < '{end.isoformat("T")}Z'
            GROUP BY device_id, sensor_type, timestamp
        ) agg
        WHERE NOT EXISTS (
            SELECT device_id, sensor_type, timestamp
            FROM `main.sensor_data_{duration}`
            WHERE
                device_id = agg.device_id AND
                sensor_type = agg.sensor_type AND
                timestamp = agg.timestamp
        )
    """

    delete_threshold = end - datetime.timedelta(minutes=THRESHOLD[duration_index])
    deletion_query = f"""
        DELETE FROM `main.sensor_data_{duration_before}`
        WHERE timestamp < '{delete_threshold.isoformat("T")}Z' 
    """

     # Run the aggregation query
    aggregation_job = client.query(aggregation_query)
    aggregation_result = aggregation_job.result()
    assert aggregation_job.state == 'DONE'
    rows_affected_agg = aggregation_job.num_dml_affected_rows
    assert type(rows_affected_agg) == int

    deletion_job = client.query(deletion_query)
    # Run the deletion query
    deletion_result = deletion_job.result()
    assert deletion_job.state == 'DONE'
    rows_affected_del = deletion_job.num_dml_affected_rows
    assert type(rows_affected_agg) == int


    # Check for errors in the jobs
    if aggregation_job.errors or deletion_job.errors:
        return {
            'success': False,
            'errors': {
                'aggregation': aggregation_job.errors,
                'deletion': deletion_job.errors,
            },
            'duration': duration,
        }
    else:
        response_data = {
            'success': True,
            'rows_affected': {
                    'aggregation': rows_affected_agg,
                    'deletion':  rows_affected_del,
                    'start': start.isoformat("T")+'Z',
                    'end': end.isoformat("T")+'Z'
                },
            'duration': duration,
        }
        logging.warning(f"Response data: {response_data}")  
        return jsonify(response_data), 200