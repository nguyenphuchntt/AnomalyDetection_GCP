# Real-time Banking Fraud Detection System

Mã nguồn và cấu hình hệ thống dự đoán bất thường thời gian thực cho giao dịch ngân hàng, triển khai trên kiến trúc Multi-cloud (GCP & AWS).

**Lớp học phần:** Điện toán đám mây - INT3319 3  
**Giảng viên hướng dẫn:** PGS.TS. Phạm Mạnh Linh, ThS. Nguyễn Xuân Trường

<br>

## Tổng quan hệ thống

Hệ thống được xây dựng để giải quyết bài toán phát hiện gian lận thẻ tín dụng, tối ưu hóa cho dữ liệu lớn và môi trường sản xuất. Quy trình hoạt động chia thành ba giai đoạn chính:

1. **Xử lý dữ liệu (Data Processing)**
2. **Huấn luyện và Triển khai mô hình (Model Training & Deployment)**
3. **Trực quan hóa và Gửi cảnh báo (Visualising and Sending Alerts)**



### Pha 1: Xử lý dữ liệu
Giai đoạn này chịu trách nhiệm thu thập, làm sạch và chuẩn hóa dữ liệu luồng trước khi lưu trữ.

* **Cloud Scheduler & Cloud Run:** Kích hoạt và thu thập dữ liệu thô từ nguồn bên ngoài theo thời gian thực, sau đó đẩy vào hệ thống.
* **Google Pub/Sub:** Message Hub trung gian với độ trễ thấp, giúp tách biệt các thành phần và phân phối dữ liệu.
* **Google Dataflow:** Xử lý luồng dữ liệu dựa trên Apache Beam. Dịch vụ này làm sạch, chuyển đổi định dạng và ghi dữ liệu vào kho lưu trữ.
* **BigQuery:** Kho dữ liệu serverless. Dữ liệu được phân tách thành bảng thô và bảng đã xử lý, sẵn sàng cho truy vấn.

### Pha 2: Huấn luyện và Triển khai mô hình
Quản lý vòng đời mô hình học máy, từ huấn luyện tự động đến vận hành trên Kubernetes.

* **Cloud Scheduler:** Lập lịch định kỳ để kích hoạt quy trình tái huấn luyện mô hình, đảm bảo tính cập nhật của thuật toán.
* **Cloud Run (Training Job):** Môi trường serverless thực thi huấn luyện mô hình XGBoost. Hệ thống tự động chia tập dữ liệu, huấn luyện và đánh giá hiệu quả.
* **Vertex AI Model Registry:** Quản lý phiên bản của các mô hình đã huấn luyện.
* **Artifact Registry:** Lưu trữ Docker Image cho các tác vụ huấn luyện và dự đoán.
* **GKE Autopilot:** Hạ tầng Kubernetes triển khai mô hình dự đoán. Hệ thống tự động quản lý node và mở rộng dựa trên tải CPU và lượng tin nhắn chờ xử lý.

### Pha 3: Trực quan hóa và Cảnh báo
Xử lý kết quả dự đoán, gửi thông báo tức thì và cung cấp giao diện quản trị.

* **Google Pub/Sub:** Tiếp nhận kết quả phân loại (Gian lận/Bình thường) từ GKE.
* **Cloud Functions:** Hàm serverless lắng nghe sự kiện từ Pub/Sub để kích hoạt quy trình cảnh báo.
* **AWS SES:** Dịch vụ gửi email quy mô lớn, chuyển cảnh báo gian lận trực tiếp đến người dùng cuối.
* **Looker Studio:** Kết nối với BigQuery để hiển thị dashboard theo dõi xu hướng và hiệu suất hệ thống.
* **AWS Elastic Beanstalk:** Hosting giao diện web, cho phép người dùng xác minh giao dịch và xem báo cáo chi tiết.

<br>

## Kết quả thực nghiệm

Hệ thống đã trải qua các bài kiểm thử hiệu năng với kết quả:

* **Độ trễ End-to-End:** Thời gian trung bình từ khi giao dịch phát sinh đến khi nhận email cảnh báo là **5.325 giây**.
* **Thông lượng:** Hệ thống (cấu hình 5 Pods) chịu tải ổn định ở mức **77.2 RPS**.
* **Khả năng chịu lỗi:** Khi tải tăng đột biến gấp 10 lần, cơ chế Auto-scaling tự động kích hoạt, đảm bảo xử lý hết hàng đợi tin nhắn mà không mất mát dữ liệu.


<br>

## Nhóm sinh viên thực hiện (Nhóm 3)

| MSSV | Họ và tên |
| :--- | :--- |
| **23021684** | **Nguyễn Anh Sơn** (Nhóm trưởng) |
| 23021620 | Thái Khắc Mạnh |
| 23021696 | Phạm Anh Tiến |
| 23021704 | Dương Anh Tuấn |
| 23021664 | Nguyễn Văn Phúc |
| 23021648 | Nguyễn Tuấn Nghĩa |
| 23021720 | Trần Duy Thành |
