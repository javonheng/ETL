#!/bin/bash
docker kill $(docker ps -q)
docker rm $(docker ps -a -q)
docker rmi -f $(docker images -aq)
yes | docker volume prune

docker-compose up airflow-init
docker-compose up