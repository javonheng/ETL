----------- README.md ---------
To spin up Docker containers:
\*\* docker-compose up

\*\* Volume in local scripts folder is mounted to /opt/airflow/scripts/ in Docker container.
Username pw: airflow

Airflow UI --> http://localhost:8080
Flower --> http://localhost:5555
Cron Expression --> https://crontab.guru/

\*\* Installed modules are located in /usr/local/lib/python3.7
https://github.com/snowflakedb/snowflake-connector-python/tree/main/tested_requirements

docker exec -it <containerId/name> /bin/bash to enter Container to access files. (remove -u 0 ; for root user)
pip show <module-name>
