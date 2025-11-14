import csv
import uuid
import os
import traceback
from flask import Flask, request
from google.cloud import pubsub_v1, storage

app = Flask(__name__)

PROJECT_ID = "int3319-477808"
TOPIC_NAME = "anomaly-data-receiver"
BUCKET_NAME = "raw-infer-data"
CSV_FILE_PATH = "creditcard.csv"
STATE_FILE_PATH = "state/row_index.txt"
ROWS_PER_RUN = 2

try:
    publisher = pubsub_v1.PublisherClient()
    storage_client = storage.Client()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_NAME)
    bucket = storage_client.bucket(BUCKET_NAME)
except Exception as e:
    print(f"LỖI NGHIÊM TRỌNG khi khởi tạo client: {e}")
    traceback.print_exc()

def get_state():
    """Lấy chỉ số dòng đã xử lý lần cuối từ GCS."""
    try:
        blob = bucket.blob(STATE_FILE_PATH)
        index_str = blob.download_as_string().decode('utf-8')
        return int(index_str)
    except Exception:
        print("Không tìm thấy file trạng thái, bắt đầu từ 0.")
        return 0

def set_state(index):
    """Lưu chỉ số dòng mới nhất vào GCS."""
    try:
        blob = bucket.blob(STATE_FILE_PATH)
        blob.upload_from_string(str(index).encode('utf-8'))
        print(f"Đã cập nhật trạng thái mới: {index}")
    except Exception as e:
        print(f"LỖI: Không thể cập nhật trạng thái: {e}")

def process_and_publish_rows():
    """
    Đọc CSV từ GCS, publish 2 dòng, và cập nhật trạng thái.
    """
    current_index = get_state()
    print(f"Bắt đầu xử lý từ dòng index: {current_index}")

    csv_blob = bucket.blob(CSV_FILE_PATH)
    if not csv_blob.exists():
        print(f"LỖI: Không tìm thấy file {CSV_FILE_PATH} trong bucket {BUCKET_NAME}.")
        return "Lỗi: Không tìm thấy file CSV", 500

    rows_published = 0
    new_index = current_index

    try:
        with csv_blob.open("r", encoding="utf-8") as csv_file:
            csv_reader = csv.reader(csv_file)

            try:
                next(csv_reader)
            except StopIteration:
                print("LỖI: File CSV rỗng.")
                return "Lỗi: File CSV rỗng", 500

            for _ in range(current_index):
                next(csv_reader)

            for row in csv_reader:
                if rows_published >= ROWS_PER_RUN:
                    break

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

                    print(f"Đã publish dòng {new_index + 1} (ID: {message_id})")
                    rows_published += 1
                    new_index += 1

                except Exception as e:
                    print(f"Lỗi khi publish dòng {new_index + 1}: {str(e)}")
                    new_index += 1

    except StopIteration:
        print("Đã đọc hết file CSV. Reset index về 0 cho lần chạy sau.")
        new_index = 0
    
    except Exception as e:
        print(f"Lỗi nghiêm trọng khi đọc stream CSV: {str(e)}")
        traceback.print_exc()
        set_state(new_index)
        return f"Lỗi: {str(e)}", 500

    set_state(new_index)
    result_message = f"Hoàn tất. Đã publish {rows_published} dòng. Index mới: {new_index}"
    print(result_message)
    return result_message, 200

@app.route("/", methods=["POST", "GET"])
def run_job():
    try:
        return process_and_publish_rows()
    except Exception as e:
        print(f"LỖI TOÀN CỤC: {e}")
        traceback.print_exc()
        return "Lỗi máy chủ nội bộ", 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))