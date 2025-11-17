import base64
import json
import functions_framework
from google.cloud import bigquery 
import datetime 

BIGQUERY_DATASET = 'fraud_dashboard_data'
BIGQUERY_TABLE = 'prediction_data'

bigquery_client = None

def get_checked_status(prediction_result):
    return True if prediction_result == 0 else False

@functions_framework.cloud_event
def main_handler(cloud_event):
    global bigquery_client
    
    if bigquery_client is None:
        try:
            bigquery_client = bigquery.Client() 
        except Exception as e:
            print(f"Error initializing BigQuery Client: {e}")
            return

    try:
        base64_data = cloud_event.data["message"]["data"]
        data_string = base64.b64decode(base64_data).decode("utf-8")
        data = json.loads(data_string)
    except Exception as e:
        print(f"Error decoding or parsing Pub/Sub message: {e}. Skipping.")
        return 

    try:
        transaction_id = data["id"]
        prediction_result = data["failure"]
        checked_status = get_checked_status(prediction_result)
        timestamp_now = datetime.datetime.now(datetime.UTC).isoformat()
        
        QUERY = """
            MERGE INTO `int3319-477808.fraud_dashboard_data.prediction_data` AS T
            USING (
                SELECT
                    @transaction_id AS transaction_id,
                    @prediction_result AS prediction_result,
                    @checked_status AS checked,
                    @timestamp_now AS timestamp_processed -- Thêm trường mới
            ) AS S
            ON T.transaction_id = S.transaction_id
            
            WHEN MATCHED THEN
                UPDATE SET
                    prediction_result = S.prediction_result,
                    checked = S.checked,
                    timestamp_processed = S.timestamp_processed -- Cập nhật timestamp
                    
            WHEN NOT MATCHED THEN
                INSERT (transaction_id, prediction_result, checked, timestamp_processed)
                VALUES (S.transaction_id, S.prediction_result, S.checked, S.timestamp_processed)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id),
                bigquery.ScalarQueryParameter("prediction_result", "INT64", prediction_result),
                bigquery.ScalarQueryParameter("checked_status", "BOOL", checked_status),
                bigquery.ScalarQueryParameter("timestamp_now", "TIMESTAMP", timestamp_now),
            ]
        )

        query_job = bigquery_client.query(QUERY, job_config=job_config)
        query_job.result() 

        print(f"Successfully MERGED record for Transaction ID: {transaction_id}.")
            
    except Exception as e:
        print(f"An error occurred during BigQuery MERGE: {e}")
        
    return