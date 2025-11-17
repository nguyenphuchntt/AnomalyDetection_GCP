from google.cloud import bigquery
from flask import jsonify

client = bigquery.Client()

STREAMING_BUFFER_DELAY_SECONDS = 7200

PROJECT_ID = client.project
DATASET_ID = "fraud_dashboard_data"
RAW_DATA_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.raw-data`" 
DATA_INPUT_TEST_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.data_input_test`"
PREDICTION_DATA_TABLE = f"`{PROJECT_ID}.{DATASET_ID}.prediction_data`"

def join_insert_and_delete_processed_data(request):

    query_script = f"""
    BEGIN
        DECLARE rows_inserted INT64 DEFAULT 0;
        DECLARE rows_deleted_input INT64 DEFAULT 0;
        DECLARE rows_deleted_pred INT64 DEFAULT 0;

        INSERT INTO {RAW_DATA_TABLE} (
            transaction_id, Time, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10, 
            V11, V12, V13, V14, V15, V16, V17, V18, V19, V20, V21, V22, 
            V23, V24, V25, V26, V27, V28, Amount, Class
        )
        SELECT 
            t1.transaction_id, t1.Time, t1.V1, t1.V2, t1.V3, t1.V4, t1.V5, t1.V6, t1.V7, t1.V8, t1.V9, t1.V10, 
            t1.V11, t1.V12, t1.V13, t1.V14, t1.V15, t1.V16, t1.V17, t1.V18, t1.V19, t1.V20, t1.V21, t1.V22, 
            t1.V23, t1.V24, t1.V25, t1.V26, t1.V27, t1.V28, t1.Amount, t1.Class
        FROM 
            {DATA_INPUT_TEST_TABLE} AS t1
        INNER JOIN 
            {PREDICTION_DATA_TABLE} AS t2
        ON 
            t1.transaction_id = t2.transaction_id
        WHERE 
            t2.checked = TRUE
            AND t2.timestamp_processed < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {STREAMING_BUFFER_DELAY_SECONDS} SECOND);
        
        SET rows_inserted = @@row_count;

        DELETE FROM {DATA_INPUT_TEST_TABLE} AS t1
        WHERE EXISTS (
            SELECT 1 
            FROM {PREDICTION_DATA_TABLE} AS t2
            WHERE t1.transaction_id = t2.transaction_id 
            AND t2.checked = TRUE
            AND t2.timestamp_processed < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {STREAMING_BUFFER_DELAY_SECONDS} SECOND)
        );
        
        SET rows_deleted_input = @@row_count;

        DELETE FROM {PREDICTION_DATA_TABLE}
        WHERE checked = TRUE
        AND timestamp_processed < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {STREAMING_BUFFER_DELAY_SECONDS} SECOND);
        
        SET rows_deleted_pred = @@row_count;

        SELECT rows_inserted, rows_deleted_input, rows_deleted_pred;
    END;
    """
    
    try:
        query_job = client.query(query_script)
        rows = list(query_job.result()) 
        
        if rows:
            stats = rows[0]
            inserted = stats.rows_inserted
            deleted_input = stats.rows_deleted_input
            deleted_pred = stats.rows_deleted_pred
        else:
            inserted = 0
            deleted_input = 0
            deleted_pred = 0

        print(f"Report: Inserted={inserted}, Deleted Input={deleted_input}, Deleted Pred={deleted_pred}")
        
        return jsonify({
            "status": "success", 
            "message": f"Xử lý thành công. Insert: {inserted}, Del Input: {deleted_input}, Del Pred: {deleted_pred}"
        }), 200
        
    except Exception as e:
        error_message = f"Lỗi BigQuery: {e}"
        print(f"{error_message}")
        
        return jsonify({
            "status": "error",
            "message": error_message
        }), 500