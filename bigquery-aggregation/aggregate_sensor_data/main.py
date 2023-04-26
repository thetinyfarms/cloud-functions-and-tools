import os
from google.cloud import bigquery #>=2.29.0
from flask import escape

def aggregate_sensor_data(request):
    """
    WIP: currently gets triggered by HTTP
    in the body of the trigger request, gets a parameter `n`
    which defines how many minutes we will aggregate over.
    i.e. n := 1, 5, 15, etc
    """
    request_json = request.get_json(silent=True)
    request_args = request.args
    # right now I'm being a bit dumb and passing an "n" for variable minutes
    if request_json and 'n' in request_json:
        n = int(request_json['n'])
    elif request_args and 'n' in request_args:
        n = int(request_args['n'])
    else:
        return 'Fatal: `n` (number of minutes to aggregate over) is undefined in request body'

    client = bigquery.Client()

    # set project ID and dataset name
    project_id = os.environ['GOOGLE_CLOUD_PROJECT'] #might be different for you
    dataset_name = 'main'

    dataset_ref = client.dataset(dataset_name, project=project_id)
    source_table_id = f'{project_id}.{dataset_name}.sensor_data'
    destination_table_id = f'{project_id}.{dataset_name}.sensor_data_{n}'

    query = f"""
        CREATE OR REPLACE TABLE {destination_table_id}
        AS
        SELECT
            TIMESTAMP_TRUNC(timestamp, {n} MINUTE) AS minute,
            ARRAY_AGG(STRUCT(timestamp, value) ORDER BY timestamp DESC LIMIT 1)[OFFSET(0)].*
        FROM
            {source_table_id}
        GROUP BY
            minute
    """

    query_job = client.query(query)
    query_job.result()

    return f'Table sensor_data_{n} created with aggregated data.'
