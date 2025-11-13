import os
import csv
import uuid  # <-- Thêm lại thư viện uuid
import itertools
from flask import Flask, Response
from google.cloud import pubsub_v1
app = Flask(__name__)

credentials_path = os.path.join("/home/tien/Project/pubsubkey.json")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

publisher = pubsub_v1.PublisherClient()
topic_path = 'projects/int3all319-477808/topics/anomaly-data-receiver'


def generate_data():
    csv_file_path = '../creditcard.csv'
    
    try:
        with open(csv_file_path, mode='r', encoding='utf-8') as file:
            csv_reader = csv.reader(file)

            try:
                next(csv_reader)
                yield "Data: Bỏ qua header.\n\n"
            except StopIteration:
                yield "Data: Lỗi - File rỗng.\n\n"
                return 

            yield f"Data: Bắt đầu publish toàn bộ file...\n\n"
            
            row_count = 0

            for row in csv_reader: 
                try:
                    if len(row) > 30:
                        row[30] = ""
                    else:
                        continue

                    transaction_id = str(uuid.uuid4())
                    new_row_list = [f'"{transaction_id}"'] + row
                    new_row_string = ",".join(new_row_list)

                    data = new_row_string.encode("utf-8")
                    future = publisher.publish(topic_path, data=data)
                    message_id = future.result()
                    
                    row_count += 1

                    if row_count % 1000 == 0:
                        yield f"Data: Đã publish {row_count} dòng (Last ID: {message_id})\n\n"
                
                except Exception as e:
                    yield f"Data: Lỗi khi publish dòng {row_count + 1} (sau header): {str(e)}\n\n"

            if row_count == 0:
                 yield f"Data: Không tìm thấy dữ liệu (sau header).\n\n"
            else:
                yield f"Data: Hoàn tất. Đã publish tổng cộng {row_count} dòng.\n\n"

    except FileNotFoundError:
        yield f"Data: Lỗi - Không tìm thấy file {csv_file_path}\n\n"
    except Exception as e:
        yield f"Data: Lỗi chung: {str(e)}\n\n"


@app.route('/data')
def sse_stream():
    return Response(generate_data(), content_type='text/event-stream')

@app.route('/')
def index():
    return "<h1>Anomaly Data Generator is Running (Full Stream)</h1>"

if __name__ == '__main__':
    app.run(host='localhost', port=8000, debug=True)