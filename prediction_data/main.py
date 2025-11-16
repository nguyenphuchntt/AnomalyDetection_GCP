import base64
import json
import functions_framework
from google.cloud import bigquery 

BIGQUERY_DATASET = 'fraud_dashboard_data'
BIGQUERY_TABLE = 'prediction_data'

bigquery_client = None

def get_checked_status(failure_flag):
    return True if failure_flag == 0 else False

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
        
        rows_to_insert = [{
            "transaction_id": transaction_id,
            "prediction_result": prediction_result,
            "checked": checked_status
        }]
        
        table_ref = bigquery_client.dataset(BIGQUERY_DATASET).table(BIGQUERY_TABLE)
        
        errors = bigquery_client.insert_rows_json(table_ref, rows_to_insert)

        if errors:
            print("Errors encountered while inserting rows:", errors)
        else:
            print(f"Successfully inserted record for ID: {transaction_id} into prediction_data.")
            
    except Exception as e:
        print(f"An error occurred during BigQuery insertion: {e}")
        
    return