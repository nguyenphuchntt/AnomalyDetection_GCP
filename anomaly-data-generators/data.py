import os
import csv  # <-- Thêm import này
import uuid
from flask import Flask, Response
from google.cloud import pubsub_v1
app = Flask(__name__)

credentials_path = os.path.join("/home/tien/Project/pubsubkey.json")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/int3319-477808/topics/anomaly-data-receiver'

def generate_data():
    csv_file_path = '../creditcard.csv'
    
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)

            header = next(csv_reader) 
          
            new_header_list = ['"transaction_id"'] + header 
            new_header_string = ",".join(new_header_list)
      
            data = new_header_string.encode("utf-8")
            future = publisher.publish(topic_path, data=data)
            message_id = future.result()
         
            yield f"Data: Published HEADER ID: {message_id}\n\n"

            for i in range(10):
                try:
                    row = next(csv_reader)
                    transaction_id = str(uuid.uuid4())

                    new_row_list = [f'"{transaction_id}"'] + row
                    new_row_string = ",".join(new_row_list)

                    data = new_row_string.encode("utf-8")
                    future = publisher.publish(topic_path, data=data)
                    message_id = future.result()
                    yield f"Data: Published ROW {i + 1} ID: {message_id}\n\n"

                except StopIteration:
                    yield "Data: Đã đọc hết file.\n\n"
                    break

    except FileNotFoundError:
        yield f"Data: Lỗi - Không tìm thấy file {csv_file_path}\n\n"
    except Exception as e:
        yield f"Data: Lỗi: {str(e)}\n\n"

@app.route('/data')
def sse_stream():
    return Response(generate_data(), content_type='text/event-stream')

@app.route('/')
def index():
    return "<h1>Anomaly Data Generator is Running</h1>"

if __name__ == '__main__':
    app.run(host='localhost', port=8000, debug=True)