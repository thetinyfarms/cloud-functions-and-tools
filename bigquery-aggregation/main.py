import logging
from flask import abort, jsonify
import datetime
from google.cloud import bigquery

DURATIONS = ["1m", "15m", "90m", "8h"]
SCHEMAS = ["latest","m", "15m", "90m", "8h"]


def aggregate_data(request, context=None):
    #if request.method != 'POST':
    #    return abort(405)
    duration = "1m" # = request.values.get('duration')
    #if duration not in DURATIONS:
    #    return abort(400)
    
    client = bigquery.Client()
    assert client.project == 'tinyfarms-website'
    now = datetime.datetime.utcnow()
    end = now.replace(second=0, microsecond=0)
    interval = int(duration[:-1])
    match duration:
        case "1m" | "15m" | "90m":
            start = end - datetime.timedelta(minutes=interval) # gets either 1 or 15
            time_scale = "MINUTE"
            truncate = "HOUR"
        case "8h":
            start = end - datetime.timedelta(hours=8)
            time_scale = "HOUR"
            truncate = "DAY"
        case _:
            raise ValueError(f"Invalid duration: {duration}")
        
    duration_before = SCHEMAS[DURATIONS.index(duration)]
    value_col = "average" if duration_before != "latest" else "value"
    time_col = "timestamp"

    delete_threshold = end - datetime.timedelta(minutes=90)
    
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
                    TIMESTAMP_TRUNC({time_col}, {truncate}), 
                    INTERVAL 
                    CAST(
                        ROUND(
                            EXTRACT({time_scale} FROM {time_col}) / {float(interval)}) * {interval} AS INTEGER)
                    {time_scale}) as timestamp
            FROM `main.sensor_data_{duration_before}`
            WHERE
                {time_col} >= '{start.isoformat("T")}Z' AND
                {time_col} < '{end.isoformat("T")}Z'
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
    if DURATIONS.index(duration) == len(DURATIONS) - 1:
        # Run the deletion query
        deletion_result = deletion_job.result()
        assert deletion_job.state == 'DONE'

    # Check for errors in the jobs
    if aggregation_job.errors or deletion_job.errors:
        return {
            'success': False,
            'errors': {
                'aggregation': aggregation_job.errors,
                'deletion': deletion_job.errors,
                'duration': duration,
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
            },
        }
        logging.warning(f"Response data: {response_data}")  
        # return jsonify(response_data), 200
    
aggregate_data(None)