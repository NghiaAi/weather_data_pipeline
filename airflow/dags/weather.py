from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pymongo
from pymongo.errors import ConnectionFailure
import psycopg2

# MongoDB Connection String
MONGO_URI = "mongodb://admin:admin@mongodb:27017/"
DB_NAME = "weather"
COLLECTION_NAME = "weather"

# API key và URL của weatherapi.com
API_KEY = '4d34e40d44364b668d495042252603'
API_URL = "http://api.weatherapi.com/v1/current.json"

# Các thành phố mà bạn muốn lấy dữ liệu thời tiết
cities_vn = [
    "An Giang", "Ba Ria-Vung Tau", "Bac Lieu", "Bac Giang", "Bac Kan", "Bac Ninh",
    "Ben Tre", "Binh Duong", "Binh Dinh", "Binh Phuoc", "Binh Thuan", "Ca Mau", 
    "Cao Bang", "Can Tho", "Da Nang", "Dak Lak", "Dak Nong", "Dien Bien", 
    "Dong Nai", "Dong Thap", "Gia Lai", "Ha Giang", "Ha Nam", "Ha Noi", "Ha Tinh", 
    "Hai Duong", "Hai Phong", "Hau Giang", "Hoa Binh", "Hung Yen", "Khanh Hoa", 
    "Kien Giang", "Kon Tum", "Lai Chau", "Lang Son", "Lao Cai", "Lam Dong", "Long An", 
    "Nam Dinh", "Nghe An", "Ninh Binh", "Ninh Thuan", "Phu Tho", "Phu Yen", 
    "Quang Binh", "Quang Nam", "Quang Ngai", "Quang Ninh", "Quang Tri", "Soc Trang", 
    "Son La", "Tay Ninh", "Thai Binh", "Thai Nguyen", "Thanh Hoa", "Thua Thien Hue", 
    "Tien Giang", "TP Ho Chi Minh", "Tra Vinh", "Tuyen Quang", "Vinh Long", "Vinh Phuc", 
    "Yen Bai"
]

# Hàm lấy dữ liệu thời tiết từ API và lưu vào MongoDB
def fetch_weather_data(city):
    # Kết nối MongoDB
    client = pymongo.MongoClient(MONGO_URI)
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]

    # Lấy dữ liệu thời tiết từ API
    url = f"{API_URL}?key={API_KEY}&q={city}&lang=vi"
    try:
        response = requests.get(url)
        data = response.json()

        if 'current' in data:
            weather_info = {
                'city': city,
                'date': datetime.now(),
                'temperature': data['current']['temp_c'],
                'condition': data['current']['condition']['text'],
                'humidity': data['current']['humidity'],
                'wind': data['current']['wind_kph'],

                'precipitation': data['current']['precip_mm']
            }

            # Lưu dữ liệu vào MongoDB
            collection.insert_one(weather_info)
            print(f"Đã lưu dữ liệu thời tiết cho {city}")
        else:
            print(f"Không có dữ liệu cho {city}")
    except requests.exceptions.RequestException as e:
        print(f"Lỗi khi lấy dữ liệu cho {city}: {e}")
    except ConnectionFailure as e:
        print(f"Lỗi kết nối MongoDB: {e}")

# Hàm kiểm tra kết nối MongoDB (Airflow HttpSensor để đợi API có sẵn)
def check_mongo_connection():
    client = pymongo.MongoClient(MONGO_URI)
    try:
        client.admin.command('ping')  # Gửi yêu cầu ping để kiểm tra kết nối
        print("Kết nối MongoDB thành công.")
    except ConnectionFailure:
        print("Lỗi kết nối MongoDB.")
        raise Exception("Không thể kết nối MongoDB.")

def add_data_to_postgres():
    client = pymongo.MongoClient("mongodb://admin:admin@mongodb:27017/")
    db = client["weather"]
    collection = db["weather"]
    data = collection.find()
    
    connection = psycopg2.connect(
            host = "postgres",
            port = "5432",
            database = "weather",
            user = "admin",
            password = "admin")
    cursor = connection.cursor()
    cursor.execute("CREATE TABLE IF NOT EXISTS weather (city TEXT, date DATE, temperature REAL, condition TEXT, humidity REAL, wind REAL, precipitation REAL)")
    for record in data:
        cursor.execute(
            """
            INSERT INTO weather (city, date, temperature, condition, humidity, wind, precipitation)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                record['city'],
                record['date'],
                record['temperature'],
                record['condition'],
                record['humidity'],
                record['wind'],
                record['precipitation']
            )
        )
    connection.commit()
    
def check_data_postgress():
    connection = psycopg2.connect(
            host = "postgres",
            port = "5432",
            database = "weather",
            user = "admin",
            password = "admin")
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM weather limit 5")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
    cursor.close()
    connection.close()
        
# Định nghĩa DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'weather_data_to_mongo',
    default_args=default_args,
    description='DAG to fetch weather data and store it into MongoDB',
    schedule_interval=timedelta(hours=6),  # Chạy mỗi 6 giờ
    start_date=datetime(2025, 3, 25),
    catchup=False,
) as dag:


    # Sensor task to check MongoDB connection
    check_mongo = PythonOperator(
        task_id='check_mongo_connection',
        python_callable=check_mongo_connection,
    )

    # Create fetch weather tasks dynamically for each city
    fetch_weather_tasks = []
    for city in cities_vn:
        fetch_weather_task = PythonOperator(
            task_id=f'fetch_weather_data_{city.replace(" ", "_")}',
            python_callable=fetch_weather_data,
            op_args=[city],  
        )
        fetch_weather_tasks.append(fetch_weather_task)

    # Task to add data to PostgreSQL
    add_data_task = PythonOperator(
        task_id='add_data_to_postgres',
        python_callable=add_data_to_postgres,
    )
    check_data = PythonOperator(
        task_id='check_data_postgress',
        python_callable=check_data_postgress,
    )
    check_mongo >> fetch_weather_tasks>> add_data_task >> check_data    
    
