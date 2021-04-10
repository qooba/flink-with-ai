#!/bin/bash

docker run -it --rm --name flink -p 8888:8888 --network app_default -v $(pwd)/data/mlflow:/mlflow -v $(pwd)/notebooks:/opt/flink/notebooks qooba/flink:dev 
