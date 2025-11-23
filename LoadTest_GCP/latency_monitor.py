# cmd
# cd LoadTest_GCP
# venv\Scripts\activate.bat
# python latency_monitor.py

import time
import json
import threading
import numpy as np
from google.cloud import pubsub_v1

# --- CẤU HÌNH ---
PROJECT_ID = "int3319-477808"
# Topic đầu ra
SUBSCRIPTION_ID = "latency-monitor-sub" 

# Biến lưu trữ toàn bộ dữ liệu (Cộng dồn)
all_latencies = []
start_time = None

def callback(message):
    global all_latencies, start_time
    
    if start_time is None:
        start_time = time.time()

    try:
        data_str = message.data.decode("utf-8")
        data_json = json.loads(data_str)
        trans_id = data_json.get("id")
        
        # Lọc tin nhắn có prefix LOCUST-
        if trans_id and "LOCUST-" in trans_id: 
            parts = trans_id.split("-")
            sent_time_ms = int(parts[1])
            receive_time_ms = int(time.time() * 1000)
            
            # Tính độ trễ
            latency_ms = receive_time_ms - sent_time_ms
            all_latencies.append(latency_ms)
        
        message.ack()
    except Exception:
        message.ack()

def print_stats_loop():
    """Vòng lặp in báo cáo mỗi 5 giây"""
    last_count = 0
    
    print(f"🚀 Đang lắng nghe... (Số liệu Full Option)")
    print(f"⏳ Chờ dữ liệu đổ về...")
    
    while True:
        time.sleep(5)
        
        current_count = len(all_latencies)
        
        # Nếu chưa có dữ liệu mới thì bỏ qua
        if current_count == 0:
            continue
            
        # --- TÍNH TOÁN SỐ LIỆU ---
        arr = np.array(all_latencies)
        
        # 1. Throughput (Thông lượng)
        # Tốc độ tức thời (trong 5s qua)
        msg_in_last_window = current_count - last_count
        current_rps = msg_in_last_window / 5.0
        last_count = current_count
        
        # Tốc độ trung bình toàn trình (từ lúc bắt đầu)
        elapsed_time = time.time() - start_time
        overall_rps = current_count / elapsed_time if elapsed_time > 0 else 0

        # 2. Latency (Độ trễ)
        avg = np.mean(arr)
        p50 = np.median(arr)
        p90 = np.percentile(arr, 90)
        p95 = np.percentile(arr, 95)
        p99 = np.percentile(arr, 99)      # <--- Của bạn đây
        p999 = np.percentile(arr, 99.9)   # <--- Dành cho những ca siêu chậm
        std_dev = np.std(arr)             # <--- Độ ổn định (càng nhỏ càng tốt)
        min_lat = np.min(arr)
        max_lat = np.max(arr)

        print("\n" + "="*60)
        print(f"⏱️  BÁO CÁO CẬP NHẬT (Tổng mẫu: {current_count})")
        print("-" * 60)
        print(f"🚀 THÔNG LƯỢNG (THROUGHPUT):")
        print(f"   ⚡ Tốc độ hiện tại:   {current_rps:.1f} req/s (Đang xử lý)")
        print(f"   🌎 Tốc độ trung bình: {overall_rps:.1f} req/s (Toàn trình)")
        print("-" * 60)
        print(f"📡 ĐỘ TRỄ (LATENCY) - ms:")
        print(f"   ✅ Trung bình (Avg):  {avg:.2f}")
        print(f"   ✅ P50 (Trung vị):    {p50:.2f}  <-- Quan trọng nhất")
        print(f"   ⚠️ P90 (90%):         {p90:.2f}")
        print(f"   ⚠️ P95 (95%):         {p95:.2f}")
        print(f"   🔥 P99 (99%):         {p99:.2f}  <-- Chỉ số cam kết SLA")
        print(f"   ☠️ P99.9 (99.9%):     {p999:.2f}")
        print(f"   📉 Min / Max:         {min_lat} / {max_lat}")
        print(f"   〰️ Độ lệch chuẩn:     {std_dev:.2f} (Jitter)")
        print("="*60)

if __name__ == "__main__":
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)
    
    stats_thread = threading.Thread(target=print_stats_loop)
    stats_thread.daemon = True
    stats_thread.start()

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    
    try:
        streaming_pull_future.result()
    except KeyboardInterrupt:
        print("\n🛑 Đã dừng monitor.")