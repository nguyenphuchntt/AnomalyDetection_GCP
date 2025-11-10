import os
from google.cloud import pubsub_v1

credentials_path = os.path.join(os.path.dirname(__file__), '..', 'credentials', 'gcp-credentials.json')
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

publisher = pubsub_v1.PublisherClient()
topic_path = ''

