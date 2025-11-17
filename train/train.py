import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import roc_auc_score, precision_score, recall_score, f1_score
from sklearn.model_selection import GridSearchCV, train_test_split
from xgboost import XGBClassifier
from sklearn.model_selection import StratifiedKFold
from google.cloud import bigquery, bigquery_storage, storage
from google.cloud import aiplatform
import os, joblib
import warnings
import argparse
import logging
from register_model import upload_model_registry
import json
from datetime import datetime

logging.basicConfig(level=logging.INFO)
warnings.filterwarnings('ignore')
parser = argparse.ArgumentParser()

parser.add_argument(
    '--model_dir', 
    help="Output model directory (GCS path like gs://your-bucket/models/ or leave empty for local)", 
    type=str, 
    default='gs://model-traning-321762/models/fraud-detection'
)

parser.add_argument(
    '--region',
    help="GCP region for Vertex AI",
    type=str,
    default='us-central1'
)

parser.add_argument(
    '--registered_model_name',
    help="Name of the registered model in Vertex AI Model Registry to compare against",
    type=str,
    default='fraud-detection-xgboost'
)

args = parser.parse_args()
arguments = args.__dict__

project_id = 'int3319-477808'
dataset_id = "fraud_dashboard_data"
table_id = "raw-data"
bq = bigquery.Client(project=project_id)
bqs = bigquery_storage.BigQueryReadClient()

# First, check the total row count
count_query = f"""
    SELECT COUNT(*) as total_rows
    FROM `{project_id}.{dataset_id}.{table_id}`
"""
count_result = bq.query(count_query).result()
total_rows = list(count_result)[0]['total_rows']
logging.info(f"Total rows in table: {total_rows:,}")

max_rows = 400000
if total_rows > max_rows:
    logging.info(f"Table has more than {max_rows:,} rows. Fetching latest {max_rows:,} rows...")
    query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        ORDER BY Time DESC
        LIMIT {max_rows}
    """
else:
    logging.info(f"Table has {total_rows:,} rows. Fetching all data...")
    query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
    """

logging.info("Fetching from BigQuery:")
df = bq.query(query).to_dataframe(bqstorage_client=bqs)
logging.info(f"Fetched {len(df):,} rows from BigQuery")

logging.info(f'Total transactions: {len(df):,}')
logging.info(f'Number of features: {df.shape[1]}')
logging.info(f"Number of fraudulent transactions: {df['Class'].sum():,}")
logging.info(f"Number of normal transactions: {(df['Class'] == 0).sum():,}")
logging.info(f"Fraud percentage: {(df['Class'].sum() / len(df) * 100):.3f}%")
X = df.drop(['Class', 'transaction_id'], axis=1)
y = df['Class']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42, stratify=y)

# Evaluate existing model from Vertex AI Model Registry
existing_model_roc_auc = None
model_directory = arguments['model_dir']
latest_version = 1

logging.info("="*60)
logging.info("CHECKING FOR EXISTING MODEL IN VERTEX AI MODEL REGISTRY")
logging.info("="*60)

# Initialize Vertex AI
aiplatform.init(project=project_id, location=arguments['region'])

# List all models with the specified display name
models = aiplatform.Model.list(
    filter=f'display_name="{arguments["registered_model_name"]}"',
    order_by="create_time desc"
)

if models:
    # Get the latest model (first in the list due to desc order)
    latest_model = models[0]
    latest_version = int(latest_model.version_id)
    logging.info(f"Evaluate model version {latest_version}")
    
    # Get the artifact URI (GCS path)
    artifact_uri = latest_model.uri
    model_directory += f'/v{latest_version + 1}'

    storage_client = storage.Client()
    
    # Download model and scalers from the artifact location
    existing_model_path = os.path.join(artifact_uri, 'model.joblib')
    existing_scalers_path = os.path.join(artifact_uri, 'scalers.joblib')
    
    model_blob = storage.blob.Blob.from_string(existing_model_path, client=storage_client)
    scalers_blob = storage.blob.Blob.from_string(existing_scalers_path, client=storage_client)
    
    if model_blob.exists() and scalers_blob.exists():
        temp_model_path = 'temp_existing_model.joblib'
        temp_scalers_path = 'temp_existing_scalers.joblib'
        
        model_blob.download_to_filename(temp_model_path)
        scalers_blob.download_to_filename(temp_scalers_path)
        
        existing_model = joblib.load(temp_model_path)
        existing_scalers = joblib.load(temp_scalers_path)

        X_test_existing = X_test.copy()
        X_test_existing['Amount'] = existing_scalers['scaler_amount'].transform(X_test_existing['Amount'].values.reshape(-1, 1))
        X_test_existing['Time'] = existing_scalers['scaler_time'].transform(X_test_existing['Time'].values.reshape(-1, 1))
        
        # Evaluate existing model
        y_pred_existing = existing_model.predict(X_test_existing)
        y_pred_proba_existing = existing_model.predict_proba(X_test_existing)[:, 1]
        
        existing_model_roc_auc = roc_auc_score(y_test, y_pred_proba_existing)
        
        logging.info("\n" + "-"*60)
        logging.info("REGISTERED MODEL PERFORMANCE ON TEST DATA:")
        logging.info("-"*60)
        logging.info(f"ROC-AUC Score: {existing_model_roc_auc:.4f}")
        logging.info(f"Precision: {precision_score(y_test, y_pred_existing):.3f}")
        logging.info(f"Recall: {recall_score(y_test, y_pred_existing):.3f}")
        logging.info(f"F1-Score: {f1_score(y_test, y_pred_existing):.3f}")
        logging.info("-"*60)
        
        # Clean up temporary files
        if os.path.exists(temp_model_path):
            os.remove(temp_model_path)
        if os.path.exists(temp_scalers_path):
            os.remove(temp_scalers_path)
    else:
        logging.warning("Model artifacts not found at registered location.")
        logging.info("Proceeding without comparison...")
else:
    logging.info(f"No registered model found with name: {arguments['registered_model_name']}")   
    model_directory += '/v1'
    logging.info(f"Model will be saved to: {model_directory}")

logging.info("="*60)
logging.info("TRAINING NEW MODEL")
logging.info("="*60)

scaler_amount = StandardScaler()
scaler_time = StandardScaler()

X_train['Amount'] = scaler_amount.fit_transform(X_train['Amount'].values.reshape(-1, 1))
X_train['Time'] = scaler_time.fit_transform(X_train['Time'].values.reshape(-1, 1))

# Transform test data using the fitted scalers (no fit, only transform)
X_test['Amount'] = scaler_amount.transform(X_test['Amount'].values.reshape(-1, 1))
X_test['Time'] = scaler_time.transform(X_test['Time'].values.reshape(-1, 1))

param_grid = {
    'learning_rate': [0.1],
    'n_estimators': [100, 150]
}

model = XGBClassifier(random_state=42)

grid_search = GridSearchCV(estimator=model, param_grid=param_grid, cv=StratifiedKFold(), scoring='f1', n_jobs=-1, verbose=1)
grid_search.fit(X_train, y_train)

logging.info(f"Best score: {grid_search.best_score_:.3f}")
logging.info(f"Best parameters: {grid_search.best_params_}")
model = grid_search.best_estimator_

logging.info("="*60)
logging.info(f"EVALUATING NEW MODEL")
logging.info("="*60)

y_pred = model.predict(X_test)
y_pred_proba = model.predict_proba(X_test)[:, 1]

roc_auc = roc_auc_score(y_test, y_pred_proba)

# Store results
results = {
    'predictions': y_pred,
    'probabilities': y_pred_proba,
    'roc_auc': roc_auc
}

logging.info(f"ROC-AUC Score: {roc_auc:.4f}")
logging.info(f"Precision: {precision_score(y_test, y_pred):.3f}")
logging.info(f"Recall: {recall_score(y_test, y_pred):.3f}")
logging.info(f"F1-Score: {f1_score(y_test, y_pred):.3f}")

should_save_model = True

if existing_model_roc_auc is not None:
    logging.info("="*60)
    logging.info("MODEL COMPARISON")
    logging.info("="*60)

    logging.info(f"Existing Model ROC-AUC: {existing_model_roc_auc:.4f}")
    logging.info(f"New Model ROC-AUC:      {roc_auc:.4f}")
    
    if roc_auc > existing_model_roc_auc:
        improvement = ((roc_auc - existing_model_roc_auc) / existing_model_roc_auc) * 100
        logging.info(f"Improvement: {improvement:.2f}%")
        should_save_model = True
    else:
        decline = ((existing_model_roc_auc - roc_auc) / existing_model_roc_auc) * 100
        logging.warning(f"Decline: {decline:.2f}%")
        logging.warning("Model will not be saved to GCS")
        should_save_model = False

# Upload model
logging.info("="*60)
logging.info(f"SAVING MODEL")
logging.info("="*60)

artifact_filename = 'model.joblib'
local_path = artifact_filename

joblib.dump(model, local_path)

scalers = {
    'scaler_amount': scaler_amount,
    'scaler_time': scaler_time
}
scalers_filename = 'scalers.joblib'

joblib.dump(scalers, scalers_filename)

metrics = {
    'roc_auc': float(roc_auc),
    'precision': float(precision_score(y_test, y_pred)),
    'recall': float(recall_score(y_test, y_pred)),
    'f1_score': float(f1_score(y_test, y_pred)),
    'timestamp': datetime.now().isoformat(),
}

metrics_filename = 'metrics.json'
with open(metrics_filename, 'w') as f:
    json.dump(metrics, f, indent=2)

logging.info(f"Model, scalers, and metrics saved locally")

if model_directory == "":
    logging.info("Running locally - model saved to current directory")
elif should_save_model:
    # Upload model
    storage_path = os.path.join(model_directory, artifact_filename)
    blob = storage.blob.Blob.from_string(storage_path, client=storage.Client())
    blob.upload_from_filename(local_path)
    logging.info(f"Model exported to: {storage_path}")
    
    # Upload scalers
    scalers_storage_path = os.path.join(model_directory, scalers_filename)
    scalers_blob = storage.blob.Blob.from_string(scalers_storage_path, client=storage.Client())
    scalers_blob.upload_from_filename(scalers_filename)
    logging.info(f"Scalers exported to: {scalers_storage_path}")
    
    # Upload metrics
    metrics_storage_path = os.path.join(model_directory, metrics_filename)
    metrics_blob = storage.blob.Blob.from_string(metrics_storage_path, client=storage.Client())
    metrics_blob.upload_from_filename(metrics_filename)
    logging.info(f"Metrics exported to: {metrics_storage_path}")

    upload_model_registry(model_directory)
else:
    logging.info("\n" + "="*60)
    logging.warning("MODEL NOT SAVED - Performance did not improve")
    logging.info("="*60)
