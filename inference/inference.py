import io
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor
import threading

from google.cloud import storage
from google.cloud import aiplatform
from google.cloud import pubsub_v1

import joblib
import json
import pandas as pd

PROJECT_ID = "int3319-477808"
REGION = "us-central1"

TOPIC_ID = "prediction-alerts"
SUBSCRIPTION_ID = "inference_sub"

MODEL_REGISTRY_NAME = "fraud-detection-xgboost"
SCALER_FILE_NAME = "scalers.joblib"
MODEL_FILE_NAME = "model.joblib"
DESTINATION_SCALER_PATH = "scalers.joblib"
DESTINATION_MODEL_PATH = "model.joblib"

executor = ThreadPoolExecutor(max_workers=5)

scaler, scaler_time, scaler_amount, model = None, None, None, None
model_lock = threading.Lock()

def download_blob(bucket_name, source_blob_path, destination_file_path):
    print(f"Downloading: gs://{bucket_name}/{source_blob_path}...")
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_path)
    blob.download_to_filename(destination_file_path)
    print("Storage object {} downloaded to {}.".format(source_blob_path, destination_file_path))

def fetch_and_download_latest_model():
    print("Fetching and downloading latest model...")
    aiplatform.init(project=PROJECT_ID, location=REGION)
    try:
        model_list = aiplatform.Model.list(
            filter=f'display_name="{MODEL_REGISTRY_NAME}"',
            order_by="create_time desc"
        )
        if not model_list:
            print(f"No models with name {MODEL_REGISTRY_NAME} found.")
            return

        lastest_model = model_list[0]
        print(f"Found lastest model. Version ID: {lastest_model.version_id}, create at: {lastest_model.create_time}")

        model_uri = urlparse(lastest_model.uri)
        bucket = model_uri.netloc
        model_prefix = model_uri.path.lstrip('/')
        if not model_prefix.endswith('/'):
            model_prefix += '/'

        scaler_blob_path = f"{model_prefix}{SCALER_FILE_NAME}"
        model_blob_path = f"{model_prefix}{MODEL_FILE_NAME}"

        download_blob(bucket, scaler_blob_path, DESTINATION_SCALER_PATH)
        download_blob(bucket, model_blob_path, DESTINATION_MODEL_PATH)
        print("Scaler and model downloaded successfully")

    except Exception as e:
        print(f"Failed to fetch latest model: {e}")


def load_model_if_needed():
    global scaler, scaler_time, scaler_amount, model

    with model_lock:
        if scaler is None or scaler_time is None or scaler_amount is None:
            scaler = joblib.load(DESTINATION_SCALER_PATH)
            scaler_time = scaler['scaler_time']
            scaler_amount = scaler['scaler_amount']
            print("StandardScaler loaded")

        if model is None:
            model = joblib.load(DESTINATION_MODEL_PATH)
            print("XGBClassifier loaded")

def parse_message_to_dataframe(message_data):
    try:
        column_names = ['transaction_id', 'Time'] + [f'V{i}' for i in range(1, 29)] + ['Amount', 'Class']
        df = pd.read_csv(io.StringIO(message_data), header=None, names=column_names)
        if df.iloc[0]['transaction_id'] == 'transaction_id':
            df = pd.read_csv(io.StringIO(message_data))
        print(f"Parsed {message_data} to dataframe.")
        return df
    except Exception as e:
        print(f"Failed to parse {message_data} to dataframe: {e}")
        raise

def preprocessing_and_predict(data_df):
    print("Preprocessing and predicting...")

    load_model_if_needed()

    transaction_id = data_df['transaction_id']
    data_df.drop(columns=['transaction_id', 'Class'], inplace=True)
    time = data_df['Time']
    amount = data_df['Amount']
    data_df['Time'] = scaler_time.transform(data_df[['Time']].values)
    data_df['Amount'] = scaler_amount.transform(data_df[['Amount']].values)
    print("Finished preprocessing.")

    prediction = model.predict(data_df).astype(int)
    prediction_proba = model.predict_proba(data_df)[:, 1]
    result_df = pd.DataFrame({
        'transaction_id': transaction_id.values,
        'prediction': prediction,
        'prediction_proba': prediction_proba,
        'time' : time.values,
        'amount' : amount.values
    })
    print("Finished predicting")
    return result_df


def publish_message(result_df):
    print("Publishing message...")
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


    for row in result_df.itertuples(index=False):
        message_data = {
            "id": row.transaction_id,
            "failure": int(row.prediction),
            "prediction_score": float(row.prediction_proba),
            "time": int(row.time),
            "amount": float(row.amount),
        }
        data_bytes = json.dumps(message_data).encode('utf-8')
        try:
            future = publisher.publish(topic_path, data_bytes)
            message_id = future.result()
            print("Message containing data of transaction with ID {} published to topic {}".format(
                message_data['id'], topic_path))
        except Exception as e:
            print("Failed to publish message containing data of transaction with ID {}: {}".format(
                message_data['id'], e))
    print("Finished publishing message.")


def process_message(message_data):
    try:
        print(f"Processing message: {message_data}")
        data_df = parse_message_to_dataframe(message_data)
        result_df = preprocessing_and_predict(data_df)

        publish_message(result_df)

        print(f"Successfully processed message: {message_data}")

    except Exception as e:
        print(f"Error processing message {message_data}: {e}")


def callback(message):
    try:
        message_data = message.data.decode('utf-8')
        print(f"Received message: {message_data}")

        message.ack()
        print(f"Message acknowledged")

        executor.submit(process_message, message_data)

    except Exception as e:
        print(f"Error in callback: {e}")
        message.nack()

def main():
    fetch_and_download_latest_model()

    try:
        load_model_if_needed()
    except Exception as e:
        print(f"Warning: Could not pre-load model: {e}")

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    flow_control = pubsub_v1.types.FlowControl(
        max_messages=5,
        max_bytes=10 * 1024 * 1024,
    )

    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback,
        flow_control=flow_control
    )

    print("Listening for messages on subscription {}...".format(subscription_path))

    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        streaming_pull_future.cancel()
        executor.shutdown(wait=True)
        print("Stopped listening for messages on subscription {}".format(subscription_path))
    except Exception as e:
        streaming_pull_future.cancel()
        executor.shutdown(wait=True)
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()