import os
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.pubsub import ReadFromPubSub
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.transforms.window import FixedWindows
import logging

credentials_path = "/home/tien/Project/dataflowkey.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

PROJECT_ID = 'int3319-477808'
REGION = 'us-central1'

BIGQUERY_TABLE = f'{PROJECT_ID}.fraud_dashboard_data.data_input_test'
SUBSCRIPTION_PATH = f'projects/{PROJECT_ID}/subscriptions/anomaly-data-receiver-sub'
TMP_BUCKET = 'gs://dataflow_temp_code/temp/'


schema_parts = ['transaction_id:STRING', 'Time:INTEGER']
for i in range(1, 29):
    schema_parts.append(f'V{i}:FLOAT')
schema_parts.append('Amount:FLOAT')
schema_parts.append('Class:INTEGER')
BQ_SCHEMA = ','.join(schema_parts)

class ProcessCSVToBQ(beam.DoFn):
    def process(self, element):
        if hasattr(element, 'data'):
            decoded_str = element.data.decode('utf-8')
        else:
            decoded_str = element.decode('utf-8')

        values = decoded_str.split(',')

        if len(values) != 32:
            logging.warning(f"Dữ liệu lỗi hoặc thiếu cột: {len(values)} cột. Raw: {decoded_str}")
            return
        
        class_val = values[31].strip()
        final_class = int(class_val) if class_val else None

        try:
            row = {
                'transaction_id': values[0].replace('"', ''),
                'Time': int(values[1]),
                'Amount': float(values[30]),
                'Class': final_class
            }

            for i in range(1, 29):
                row[f'V{i}'] = float(values[i + 1])

            print(f"Processed row: {row['transaction_id']}")
            
            logging.info(f"--- DEBUG TYPE CHECK ---")
            logging.info(f"transaction_id: Val='{row['transaction_id']}' - Type={type(row['transaction_id'])}")
            logging.info(f"Time: Val={row['Time']} - Type={type(row['Time'])}")
            logging.info(f"V1: Val={row['V1']} - Type={type(row['V1'])}")
            logging.info(f"Amount: Val={row['Amount']} - Type={type(row['Amount'])}")
            logging.info(f"Class: Val={row['Class']} - Type={type(row['Class'])}")
            logging.info(f"------------------------")
            
            yield row

        except Exception as e:
            logging.error(f"Lỗi parse dữ liệu: {e} - Raw: {decoded_str}")
            return

def run_pipeline():
    beam_options = PipelineOptions([
        '--runner=DataflowRunner',
        f'--project={PROJECT_ID}',
        f'--region={REGION}',
        f'--temp_location={TMP_BUCKET}',
        '--streaming',
        f'--service_account_email=data-flow@int3319-477808.iam.gserviceaccount.com'
    ])

    with beam.Pipeline(options=beam_options) as pipeline:
        messages = (
            pipeline
            | 'Read from PubSub' >> ReadFromPubSub(
                subscription=SUBSCRIPTION_PATH,
                with_attributes=True
            )
            | 'Window 10s' >> beam.WindowInto(FixedWindows(10))
        )

        processed_data = (
            messages
            | 'Parse CSV to Row' >> beam.ParDo(ProcessCSVToBQ())
        )

        processed_data | 'Write to BigQuery' >> WriteToBigQuery(
            table=BIGQUERY_TABLE,
            schema=BQ_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run_pipeline()