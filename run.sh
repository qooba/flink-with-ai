#!/bin/bash

#docker network create -d bridge app_default

docker run -d --rm -p 2181:2181 --name zookeeper --network app_default -e ZOOKEEPER_CLIENT_PORT=2181 confluentinc/cp-zookeeper

docker run -d --rm -p 9092:9092 --name kafka --network app_default -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 confluentinc/cp-kafka

#docker run -d --rm -p 5432:5432 --name postgres -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=changeme -e PGDATA=/data/postgres --network app_default -v $(pwd)/data/postgres:/data/postgres postgres

docker run --name mysql --rm -e MYSQL_ROOT_PASSWORD=my-secret-pw -v $(pwd)/data/mysql:/val/lib/mysql --network app_default -d mysql

docker run --name mlflow --rm -p 5000:5000 --network app_default -v $(pwd)/data/mlflow:/mlflow  -d qooba/mlflow:dev
