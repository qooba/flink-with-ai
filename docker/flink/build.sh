#!/bin/bash

docker build -t qooba/flink:dev .
docker build -f Dockerfile.dev -t qooba/flink:dev_ .
