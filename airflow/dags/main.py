from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pymongo
import psycopg2
import logging
from pymongo.errors import ConnectionFailure
import sys
sys.path.append("/opt/airflow/config")

from config import MONGO_URI, DB_NAME, COLLECTION_NAME, POSTGRES_CONFIG, API_KEY, API_URL, CITIES_VN
if not all([MONGO_URI, DB_NAME, COLLECTION_NAME, POSTGRES_CONFIG, API_KEY, API_URL, CITIES_VN]):
    raise ValueError("Thiếu cấu hình trong config.py")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_mongo_client():
    try:
        client = pymongo.MongoClient(MONGO_URI)
        client.admin.command("ping")  
        return client
    except ConnectionFailure as e:
        logger.error("Không thể kết nối MongoDB: %s", e)
        client.close()
        raise

def check_mongo_connection():
    get_mongo_client()
    logger.info("MongoDB kết nối thành công.")

def get_past_weather_data(city, date):
    url = f"{API_URL}/history.json?key={API_KEY}&q={city}&dt={date}&lang=vi"
    try:
        response = requests.get(url)
        response.raise_for_status()  # Kiểm tra lỗi HTTP
        data = response.json()

        if 'forecast' in data:
            forecast_day = data['forecast']['forecastday'][0]['day']
            weather_info = {
                'city': city,
                'date': date,
                'temperature': forecast_day['avgtemp_c'],
                'condition': forecast_day['condition']['text'],
                'humidity': forecast_day['avghumidity'],
                'wind': forecast_day['maxwind_kph'],
                'precipitation': forecast_day['totalprecip_mm'],
            }
            return weather_info
        else:
            logger.warning(f"Không có dữ liệu cho {city} vào ngày {date}")
            return None

    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi API khi lấy dữ liệu cho {city} vào ngày {date}: {e}")
        return None
    
def get_current_weather_data(city):
    """Lấy dữ liệu thời tiết hiện tại từ WeatherAPI."""
    url = f"{API_URL}/current.json?key={API_KEY}&q={city}&lang=vi"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if 'current' in data:
            weather_info = {
                'city': city,
                'date': datetime.now(),  
                'temperature': data['current']['temp_c'],
                'condition': data['current']['condition']['text'],
                'humidity': data['current']['humidity'],
                'wind': data['current']['wind_kph'],
                'precipitation': data['current']['precip_mm'],
            }
            return weather_info
        else:
            logger.warning(f"Không có dữ liệu hiện tại cho {city}")
            return None
    except requests.exceptions.RequestException as e:
        logger.error(f"Lỗi API khi lấy dữ liệu hiện tại cho {city}: {e}")
        return None

def fetch_weather_data():
    client = get_mongo_client()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    
    with get_postgres_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS weather (
                    city TEXT,
                    date TIMESTAMP,
                    temperature REAL,
                    condition TEXT,
                    humidity REAL,
                    wind REAL,
                    precipitation REAL
                )
            """)
            cursor.execute("SELECT MAX(date) FROM weather")
            last_date = cursor.fetchone()[0]
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)  # Ngày hiện tại (0h)

    if last_date is None:
        start_date = today - timedelta(days=30)
    else:
        start_date = last_date + timedelta(days=1)

    end_date = today

    if start_date > end_date:
        logger.info("Dữ liệu đã bắt kịp ngày hiện tại. Chỉ lấy dữ liệu hôm nay.")
        for city in CITIES_VN:
            weather_data = get_current_weather_data(city)
            if weather_data and not collection.find_one({"city": city, "date": {"$gte": today}}):
                collection.insert_one(weather_data)
                logger.info(f"Đã lưu dữ liệu hiện tại cho {city}")
        return

    date_range = [start_date + timedelta(days=x) for x in range((end_date - start_date).days + 1)]
    for date in date_range:
        date_str = date.strftime("%Y-%m-%d")
        for city in CITIES_VN:
            if date < today:
                weather_data = get_past_weather_data(city, date_str)
            else:
                weather_data = get_current_weather_data(city)

            if weather_data and not collection.find_one({"city": city, "date": weather_data['date']}):
                collection.insert_one(weather_data)
                logger.info(f"Đã lưu dữ liệu cho {city} vào ngày {date_str}")
            else:
                logger.warning(f"Dữ liệu cho {city} vào ngày {date_str} đã tồn tại hoặc không có.")
                
def get_postgres_connection():
    return psycopg2.connect(**POSTGRES_CONFIG)

def add_data_to_postgres():
    client = get_mongo_client()
    db = client[DB_NAME]
    collection = db[COLLECTION_NAME]
    data = collection.find()

    with get_postgres_connection() as conn:
        with conn.cursor() as cursor:
            for record in data:
                try:
                    cursor.execute("""
                        INSERT INTO weather (city, date, temperature, condition, humidity, wind, precipitation)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        record["city"], record["date"], record["temperature"], 
                        record["condition"], record["humidity"], record["wind"], record["precipitation"]
                    ))
                except Exception as e:
                    logger.error(f"Lỗi khi chèn bản ghi {record['city']} - {record['date']}: {e}")
                    conn.rollback()
                else:
                    conn.commit()
            logger.info("Dữ liệu đã được chèn vào PostgreSQL.")

def check_data_postgres():
    with get_postgres_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM weather LIMIT 5")
            rows = cursor.fetchall()
            for row in rows:
                logger.info(row)

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "weather_data_pipeline",
    default_args=default_args,
    description="DAG thu thập dữ liệu thời tiết và lưu vào MongoDB & PostgreSQL",
    schedule_interval=timedelta(hours=6),
    start_date=datetime(2025, 3, 25),
    catchup=False,
) as dag:

    check_mongo = PythonOperator(
        task_id="check_mongo_connection",
        python_callable=check_mongo_connection,
    )

    fetch_weather_task = PythonOperator(
        task_id="fetch_weather_data",
        python_callable=fetch_weather_data,
    )

    add_data_task = PythonOperator(
        task_id="add_data_to_postgres",
        python_callable=add_data_to_postgres,
    )

    check_data = PythonOperator(
        task_id="check_data_postgres",
        python_callable=check_data_postgres,
    )

    check_mongo >> fetch_weather_task >> add_data_task >> check_data