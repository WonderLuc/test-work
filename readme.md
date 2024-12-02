## Description
This is simple test work

## Installation
* First clone the repo
 
```
git clone 
cd test-work/
```
* Then use docker compose
 
```
docker-compose up
```
* After installation. Open browser at localhost:8080
> password and login for all services - airflow

* Go to the connections and add postgres connection
  * Connection Id - postgres
  * host - host.docker.internal
  * Database - airflow
  * Login - airflow
  * Password - airflow

* Now you can run DAG named simple-etl