import uuid
import os
import traceback
from flask import Flask, request
from google.cloud import pubsub_v1

app = Flask(__name__)

PROJECT_ID = "int3319-477808"
TOPIC_NAME = "anomaly-data-receiver"

try:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
except Exception as e:
    print(f"LỖI khi khởi tạo client Pub/Sub: {e}")
    traceback.print_exc()

def process_and_publish_data(incoming_data):
    if not isinstance(incoming_data, list):
        return "Lỗi: Dữ liệu đầu vào phải là một JSON array", 400

    rows_published = 0
    errors_count = 0

    for row in incoming_data:
        if not isinstance(row, list):
            print(f"Lỗi: Mục dữ liệu không phải là danh sách, bỏ qua: {row}")
            errors_count += 1
            continue
        
        try:
            transaction_id = str(uuid.uuid4())
            new_row_list = [f'"{transaction_id}"'] + row_str
            new_row_string = ",".join(new_row_list)
            data = new_row_string.encode("utf-8")
            
            future = publisher.publish(topic_path, data=data)
            message_id = future.result()

            print(f"Đã publish message (ID: {message_id})")
            rows_published += 1

        except Exception as e:
            print(f"Lỗi khi xử lý hoặc publish dòng: {str(e)}")
            traceback.print_exc()
            errors_count += 1

    result_message = f"Hoàn tất xử lý request. Đã publish: {rows_published} dòng. Lỗi: {errors_count} dòng."
    print(result_message)

    return result_message, 200

@app.route("/", methods=["POST"])
def run_job():
    """
    Entry point của Flask app.
    Nhận dữ liệu JSON từ POST request và xử lý.
    """
    try:
        incoming_data = request.json
        
        if not incoming_data:
            return "Lỗi: Không tìm thấy nội dung JSON trong request body.", 400

        return process_and_publish_data(incoming_data)

    except Exception as e:
        print(f"LỖI TOÀN CỤC khi xử lý request: {e}")
        traceback.print_exc()
        return "Lỗi máy chủ nội bộ", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
