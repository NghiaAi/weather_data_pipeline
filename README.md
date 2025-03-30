# Weather Data Pipeline

## Mô tả dự án

Dự án này sử dụng Apache Airflow để thu thập dữ liệu thời tiết từ WeatherAPI, lưu trữ vào MongoDB và PostgreSQL.
Hệ thống hoạt động theo hai chế độ:

1. **Lần chạy đầu tiên**: Cào dữ liệu thời tiết từ 30 ngày trước đến hiện tại.
2. **Những lần chạy sau**: Chỉ thu thập dữ liệu thời tiết của ngày hiện tại.

## Công nghệ sử dụng

- **Apache Airflow**: Quản lý và lên lịch các tác vụ.
- **MongoDB**: Lưu trữ dữ liệu thô.
- **PostgreSQL**: Lưu trữ dữ liệu có cấu trúc.
- **Docker & Docker Compose**: Đóng gói và triển khai.
- **WeatherAPI**: Lấy dữ liệu thời tiết.

## Cấu trúc thư mục

```
project-root/
│── airflow/
│   ├── dags/                # Chứa file DAG của Airflow
│   ├── logs/                # Lưu trữ log của Airflow
│   ├── config/              # Chứa file config.py
│── docker-compose.yml       # Docker Compose cấu hình dịch vụ
├── Dockerfile               # File cài đặt môi trường Airflow
│── README.md                # Tài liệu hướng dẫn
```

## Cài đặt và chạy dự án

### 1. Cấu hình API Key

Mở file `airflow/config/config.py` và cập nhật API_KEY (API lấy từ trang Web: https://www.weatherapi.com/my/)

```python
API_KEY = "your_api_key_here"
```

### 2. Khởi chạy hệ thống

```sh
docker-compose up -d --build
```

Lệnh này sẽ:

- Tạo các container cho MongoDB, PostgreSQL, Airflow.
- Khởi tạo cơ sở dữ liệu PostgreSQL và MongoDB.
- Cài đặt Airflow.

### 3. Truy cập giao diện Airflow

Mở trình duyệt và truy cập: `http://localhost:8080`

- Username: `airflow`
- Password: `airflow`

## Các DAG trong Airflow

DAG chính của hệ thống có tên `weather_data_pipeline`, bao gồm các task:

1. **check_mongo_connection**: Kiểm tra kết nối MongoDB.
2. **fetch_weather_data**: Lấy dữ liệu thời tiết từ API.
3. **add_data_to_postgres**: Chuyển dữ liệu từ MongoDB sang PostgreSQL.
4. **check_data_postgres**: Kiểm tra dữ liệu trong PostgreSQL.

## Kiểm tra dữ liệu trong PostgreSQL

Mở terminal và truy vấn dữ liệu:

```sh
docker exec -it postgres psql -U admin -d weather
```

Chạy lệnh sau để kiểm tra dữ liệu:

```sql
SELECT * FROM weather LIMIT 10;
```

## Dừng và xóa container

```sh
docker-compose down -v
```

Lệnh này sẽ xóa tất cả container và volume của dự án.

## Khắc phục sự cố

1. **Airflow không chạy được DAG**

   - Đảm bảo Airflow đã khởi động: `docker ps`
   - Kiểm tra log của Airflow: `docker logs airflow-webserver`

2. **Không kết nối được MongoDB/PostgreSQL**

   - Kiểm tra log MongoDB: `docker logs mongodb`
   - Kiểm tra log PostgreSQL: `docker logs postgres`

3. **Lỗi API Key không hợp lệ**
   - Kiểm tra API_KEY trong `config.py`.
   - Đảm bảo API_KEY có quyền truy cập dữ liệu lịch sử thời tiết.

---

Dự án này giúp tự động hóa việc thu thập và lưu trữ dữ liệu thời tiết, hỗ trợ phân tích và dự báo thời tiết trong tương lai. 🚀
