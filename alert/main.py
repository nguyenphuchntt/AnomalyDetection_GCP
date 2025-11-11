import os
import boto3
import base64
import json
from botocore.exceptions import ClientError
import functions_framework

# ENV VARIABLES
AWS_REGION = os.environ.get('AWS_REGION')
SENDER_EMAIL = os.environ.get('SENDER_EMAIL')
RECIPIENT_EMAIL = os.environ.get('RECIPIENT_EMAIL')
AWS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY') 

ses_client = None

@functions_framework.cloud_event
def main_handler(cloud_event):
    global ses_client
    # Decode incomming Cloud Event data
    try:
        base64_data = cloud_event.data["message"]["data"]
        data_string = base64.b64decode(base64_data).decode("utf-8")
        print(f"Current data: {data_string}")
        data = json.loads(data_string)
    except KeyError:
        print("Message does not contain 'data' field. Skipping processing.")
        return
    except Exception as e:
        print(f"Error occurred while processing data: {e}")
        return 

    ALERT_COLUMN = "failure"
    if ALERT_COLUMN not in data or data[ALERT_COLUMN] != 1:
        print(f"(flag={data.get(ALERT_COLUMN)}). Skipping processing.")
        return
    print(f"Found alert in data: {data}")

    ## AWS SES CLIENT INIT
    if ses_client is None:
        print("Initializing AWS SES Client...")
        try:
            ses_client = boto3.client('ses',
                                    region_name=AWS_REGION,
                                    aws_access_key_id=AWS_KEY_ID,
                                    aws_secret_access_key=AWS_SECRET_KEY
                                    )
            print("AWS SES Client initialized successfully.")
        except Exception as e:
            print(f"Error occurred while initializing AWS SES Client: {e}")
            return
    try:
        transaction_id = data.get("id", "Unknown")
        subject = f"[Big alert][Auto Fraud Detect System] Found fraud in transaction- ID: {transaction_id}"
        body_text = (f"Found a fraud in transaction.\n\n"
                    f"Content:\n"
                    f" Transaction ID: {transaction_id}\n\n"
                    f"Please investigate immediately.")

        print(f"Sending email for transaction ID: {transaction_id} to {RECIPIENT_EMAIL}")

        ses_client.send_email(
            Destination={'ToAddresses': [RECIPIENT_EMAIL]},
            Message={
                'Body': {'Text': {'Charset': "UTF-8", 'Data': body_text}},
                'Subject': {'Charset': "UTF-8", 'Data': subject},
            },
            Source=SENDER_EMAIL,
        )
        print("Finished sending email alert.")
    except ClientError as e:
        print(f"AWS SES Error: {e.auto_str(e.response['Error']['Message'])}")
    except Exception as e:
        print(f"Error occurred while sending email alert: {e}")

    return