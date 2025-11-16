import os
import base64
import json
import functions_framework
from google.cloud import bigquery 

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
        transaction_id_value = data["id"]
        prediction_result_value = data["failure"]
        failure_flag = data["failure"]
        prediction_score_value = data["prediction_score"]
        amount_value = data["amount"]
        time_value = data["time"]
        
        actual_result_value = get_actual_result(failure_flag)
        
        rows_to_insert = [{
            "transaction_id": transaction_id_value,
            "prediction_score": prediction_score_value,
            "prediction_result": prediction_result_value,
            "actual_result": actual_result_value,
            "amount": amount_value,
            "time": time_value
        }]
        
        table_ref = bigquery_client.dataset(BIGQUERY_DATASET).table(BIGQUERY_TABLE)
        
        errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)

        if errors:
            print("Errors encountered while inserting rows:")
            print(errors)
        else:
            print(f"Successfully inserted record for Transaction ID: {data['id']}.")
            
    except Exception as e:
        print(f"An error occurred during BigQuery insertion: {e}")
        
    return