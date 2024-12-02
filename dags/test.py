from faker import Faker
import re
import json
from datetime import datetime, timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook


with DAG(
    "simple_etl",
    default_args={
        "depends_on_past": False,
        "email": ["airflow@example.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="fake enough data in db",
    schedule=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["extract"],
) as dag:
    
    fake = Faker('en_US')
    

    def generate_people(ti):
        peoples = []
        for _ in range(100):
            profile = fake.simple_profile()
            # Transfrom it there because XCOM dont correct work with arrays
            profile['birthdate'] = profile['birthdate'].strftime('%m/%d/%Y')
            peoples.append(profile)
        return ti.xcom_push(key="raw_data", value=json.dumps(peoples))

    
    def clear_data(ti):
        peoples = json.loads(ti.xcom_pull(key="raw_data", task_ids="load_data"))
        for i in range(len(peoples)):
            people = peoples[i]
            fist_name = people['name'].split()[0]
            last_name = people['name'].split()[1]
            post_code = re.search("\d+$", people['address'])
            post_code = int(post_code.group(0)) if post_code else 0
            birthday = people['birthdate']
            email = people['mail']
            peoples[i] = (fist_name, last_name, post_code, birthday, email)
        ti.xcom_push(key="clean_data", value=json.dumps(peoples))
        
    def send_data(ti):
        peoples = json.loads(ti.xcom_pull(key="clean_data", task_ids="transform_data"))
        pg_hook = PostgresHook(postgres_conn_id='postgres')
        connection = pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS peoples (
                people_id serial PRIMARY KEY,
                fist_name VARCHAR(30) NOT NULL,
                last_name VARCHAR(30) NOT NULL,
                post_code INT NOT NULL,
                birthday DATE NOT NULL,
                email TEXT NOT NULL
            );
        """)
        for people in peoples:
            cursor.execute("INSERT INTO peoples (fist_name, last_name, post_code, birthday, email) VALUES( %s, %s, %s, %s, %s)",
                          people)
        
        connection.commit()
        cursor.close()
        connection.close()  

    t1 = PythonOperator(
        task_id="load_data",
        python_callable=generate_people
    )

    t2 = PythonOperator(
        task_id="transform_data",
        python_callable=clear_data
    )

    t4= PostgresOperator(
        task_id="show_result",
        postgres_conn_id="postgres",
        sql="SELECT * FROM peoples;"
    )

    t3 = PythonOperator(
        task_id="create_req",
        python_callable=send_data
    )     
    
    
 
t1 >> t2 >> t3 >> t4
