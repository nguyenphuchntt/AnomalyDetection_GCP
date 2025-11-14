import os
import base64
import json
import functions_framework
from google.cloud import bigquery 

BIGQUERY_DATASET = 'fraud_dashboard_data'
BIGQUERY_TABLE = 'history_db'

bigquery_client = None

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
        rows_to_insert = [{
            "transaction_id": data["transaction_id"],
            "prediction_score": data["prediction_score"],
            "prediction_result": data["prediction_result"],
            "actual_result": data["actual_result"]
        }]
        
        table_ref = bigquery_client.dataset(BIGQUERY_DATASET).table(BIGQUERY_TABLE)
        
        errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)

        if errors:
            print("Errors encountered while inserting rows:")
            print(errors)
        else:
            print(f"Successfully inserted record for Transaction ID: {data['transaction_id']}.")
            
    except Exception as e:
        print(f"An error occurred during BigQuery insertion: {e}")
        
    return