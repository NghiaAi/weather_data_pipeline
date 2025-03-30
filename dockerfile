FROM apache/airflow:2.10.2
RUN pip install pymongo 
# apache-airflow-providers-postgres pandas requests 