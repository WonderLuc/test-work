from faker import Faker
import re
from datetime import datetime
from airflow.decorators import task, dag
from airflow.hooks.postgres_hook import PostgresHook

fake = Faker('en_US')

@dag(start_date=datetime(2024, 11,11), schedule_interval='@daily', catchup=False)
def simple_etl_decorated():
    
    @task(task_id="extract")
    def generate_people():
        peoples = []
        for _ in range(100):
            profile = fake.simple_profile()           
            peoples.append(profile)
        return peoples
    
    @task(task_id="clearing")
    def clear_data(peoples):
        for i in range(len(peoples)):
            people = peoples[i]
            fist_name = people['name'].split()[0]
            last_name = people['name'].split()[1]
            post_code = re.search("\d+$", people['address'])
            post_code = int(post_code.group(0)) if post_code else 0
            birthday = people['birthdate'].strftime('%m/%d/%Y')
            email = people['mail']
            peoples[i] = (fist_name, last_name, post_code, birthday, email)
        return peoples
    
    @task(task_id="transforming")
    def send_data(peoples):
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
        
    extract = generate_people()
    transform = clear_data(extract)
    send_data(transform)


simple_dag = simple_etlite()
