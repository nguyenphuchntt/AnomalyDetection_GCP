# cmd
# cd LoadTest_GCP
# venv\Scripts\activate.bat
# gcloud auth application-default login
# locust -f locustfile.py

import uuid
import time
import random
from locust import User, task, constant, events
from google.cloud import pubsub_v1

# --- CẤU HÌNH ---
PROJECT_ID = "int3319-477808"
TOPIC_ID = "anomaly-data-receiver"

try:
    publisher = pubsub_v1.PublisherClient(transport="rest")
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
except Exception as e:
    print(f"Lỗi Init: {e}")
    exit()

class PubSubUser(User):
    # YÊU CẦU: 1 User - 1s - 1 Req
    # constant(1) nghĩa là bắn xong nghỉ đúng 1 giây rồi bắn tiếp
    wait_time = constant(1)

    @task
    def publish_stream(self):
        start_perf_counter = time.perf_counter()
        
        # --- 1. Tạo dữ liệu chuẩn (Giống dữ liệu thật để Server không lỗi) ---
        current_time_millis = int(time.time() * 1000)
        
        # ID có chứa timestamp để đo Latency bằng file monitor
        transaction_id = f"LOCUST-{current_time_millis}-{str(uuid.uuid4())[:8]}"
        
        # Các trường giả lập (Hard-code các giá trị đã test thành công)
        time_col = 55363 
        # Random nhẹ các cột V để dữ liệu đỡ bị trùng lặp hoàn toàn
        v_string = ",".join([f"{random.gauss(0, 1):.4f}" for _ in range(28)])
        amount = 100.50
        class_col = 0
        
        csv_string = f"{transaction_id},{time_col},{v_string},{amount},{class_col}"
        data = csv_string.encode("utf-8")
        
        # --- 2. Gửi tin ---
        try:
            future = publisher.publish(topic_path, data)
            future.result(timeout=10)
            
            response_time = (time.perf_counter() - start_perf_counter) * 1000
            events.request.fire(
                request_type="PubSub",
                name="send_1s_1req", # Tên task
                response_time=response_time,
                response_length=len(data),
                exception=None,
            )

        except Exception as e:
            response_time = (time.perf_counter() - start_perf_counter) * 1000
            events.request.fire(
                request_type="PubSub",
                name="send_1s_1req",
                response_time=response_time,
                response_length=0,
                exception=e,
            )