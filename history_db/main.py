import os
import base64
import json
import functions_framework
from google.cloud import bigquery 
import datetime 

BIGQUERY_DATASET = 'fraud_dashboard_data'
BIGQUERY_TABLE = 'history_db'

bigquery_client = None

def get_actual_result(failure_flag):
    if failure_flag == 0:
        return 0
    else:
        return None

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
        prediction_score = data["prediction_score"]
        amount = data["amount"]
        time = data["time"]
        actual_result = get_actual_result(prediction_result)
        timestamp_now = datetime.datetime.now(datetime.UTC).isoformat() 
        
        QUERY = """
            MERGE INTO `int3319-477808.fraud_dashboard_data.history_db` AS T
            USING (
                SELECT
                    @transaction_id AS transaction_id,
                    @prediction_score AS prediction_score,
                    @prediction_result AS prediction_result,
                    @actual_result AS actual_result,
                    @amount AS amount,
                    @time AS time,
                    @timestamp_now AS timestamp_processed -- Thêm trường mới
            ) AS S
            ON T.transaction_id = S.transaction_id
            
            WHEN MATCHED THEN
                UPDATE SET
                    prediction_score = S.prediction_score,
                    prediction_result = S.prediction_result,
                    actual_result = S.actual_result,
                    amount = S.amount,
                    time = S.time,
                    timestamp_processed = S.timestamp_processed -- Cập nhật timestamp
                    
            WHEN NOT MATCHED THEN
                INSERT (transaction_id, prediction_score, prediction_result, actual_result, amount, time, timestamp_processed)
                VALUES (S.transaction_id, S.prediction_score, S.prediction_result, S.actual_result, S.amount, S.time, S.timestamp_processed)
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("transaction_id", "STRING", transaction_id),
                bigquery.ScalarQueryParameter("prediction_score", "FLOAT64", prediction_score),
                bigquery.ScalarQueryParameter("prediction_result", "INT64", prediction_result),
                bigquery.ScalarQueryParameter("actual_result", "INT64", actual_result), 
                bigquery.ScalarQueryParameter("amount", "FLOAT64", amount),
                bigquery.ScalarQueryParameter("time", "INT64", time),
                bigquery.ScalarQueryParameter("timestamp_now", "TIMESTAMP", timestamp_now),
            ]
        )

        query_job = bigquery_client.query(QUERY, job_config=job_config)
        query_job.result() 

        print(f"Successfully MERGED record for Transaction ID: {transaction_id}.")
            
    except Exception as e:
        print(f"An error occurred during BigQuery MERGE: {e}")
        
    return