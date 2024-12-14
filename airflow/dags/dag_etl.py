import pandas as pd
import requests
import csv
import urllib.request
import os
import sqlalchemy as sa
from airflow.models.param import Param
from airflow.decorators import dag, branch_task, task
from airflow.operators.empty import EmptyOperator


@dag(
    dag_id            = "dag_etl",
    description       = "ini adalah dag untuk ETL",
    tags              = ["assignment_dibimbing"],
    default_args      = { # bisa set nama owner disini
        "owner": "Fitran",
    },
    
    params = {
        "url": Param("default param1", description="masukan url dimana source data berada"),
        "source_file": Param("default param2", description="masukan source file yang ingin diekstrak (contoh csv/api)")
    }
)

def dag_etl():
    
    start_task = EmptyOperator(task_id="start_task")
    
    @branch_task
    def choose_extract_task(source_file):
        return "extract_from_api" if source_file == 'api' else "extract_from_csv"
       
        # if source_file == 'api':
        #     return "extract_from_api"
        # else:
        #     return "extract_from_csv"
    
    @task
    # cara menggunakan inputan user sebagai parameter (1)
    def extract_from_csv(url):
        filename = 'addresses.csv'
        urllib.request.urlretrieve(url, filename)
    
        with open(filename, "r") as f:
            reader = csv.DictReader(f)
            data   = [row for row in reader]
       
        df = pd.DataFrame(data)
    
        folder_path = './data'
    
        file_path = os.path.join(folder_path, filename)
    
        df.to_csv(file_path, index=False) 
        
        return filename
        
    @task
    def extract_from_api(url):
        filename = 'news.csv'
        response = requests.get(url)
        data = response.json()
        
        records = []
        
        for category, articles in data.items():
            for article in articles:
                record = {
                    'title': article['title'],
                    'category': category,
                    'link': article['link'],
                    'og': article['og'],
                    'source': article['source'],
                    'source_icon': article['source_icon']
            }
                records.append(record)
    
        file_path = f'./data/{filename}'
    
        df = pd.DataFrame(records)
        df.to_csv(file_path, index=False)
        
        return filename
    
    #set trigger rule
    @task(trigger_rule='one_success')
    # cara menggunakan ti.xcom, untuk mendapatkan data dari task lain
    def load_to_sqlite(ti):
        selected_task_id = ti.xcom_pull(task_ids='choose_extract_task')
        filename = ti.xcom_pull(task_ids=selected_task_id)
        
        df = pd.read_csv(f'./data/{filename}')
        engine     = sa.create_engine("sqlite:///data/dibimbing.db")
    
        table_name = filename.split(".")[0]
        with engine.begin() as conn:
            print("Successfully connected to SQLite.")
            df.to_sql(table_name, conn, index=False, if_exists="replace")
    
    end_task = EmptyOperator(task_id="end_task")
    
    # cara menggunakan inputan user sebagai parameter (2)
    start_task >> choose_extract_task(source_file = "{{ params['source_file'] }}") \
        >> [extract_from_csv(url = "{{ params['url'] }}"), extract_from_api(url = "{{ params['url'] }}")] \
        >> load_to_sqlite() >>  end_task

dag_etl()
