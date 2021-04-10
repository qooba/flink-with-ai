# I have based on the Dockerfile from: https://github.com/pyflink/playgrounds/blob/master/image/Dockerfile
FROM flink:1.12.2-scala_2.11

# Install python
RUN set -ex; \
  apt-get update; \
  apt-get -y install python3; \
  apt-get -y install python3-pip; \
  apt-get -y install python3-dev; \
  ln -s /usr/bin/python3 /usr/bin/python; \
  ln -s /usr/bin/pip3 /usr/bin/pip

# Install pyflink
RUN set -ex; \
  apt-get update; \
  python -m pip install --upgrade pip; \
  pip install apache-flink

ARG FLINK_VERSION=1.12.0

# FLINK FORMATS
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/1.12.0/flink-sql-avro-confluent-registry-1.12.0.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-csv/1.12.2/flink-csv-1.12.2.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.12.2/flink-json-1.12.2.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-json/1.12.2/flink-json-1.12.2.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-parquet_2.12/1.12.2/flink-parquet_2.12-1.12.2.jar

# FLINK CONNECTORS
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka_2.11/1.12.0/flink-sql-connector-kafka_2.11-1.12.0.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-jdbc_2.11/1.12.0/flink-connector-jdbc_2.11-1.12.0.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hive-3.1.2_2.11/1.12.0/flink-sql-connector-hive-3.1.2_2.11-1.12.0.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-hbase-2.2_2.11/1.12.0/flink-sql-connector-hbase-2.2_2.11-1.12.0.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch6_2.11/1.12.0/flink-sql-connector-elasticsearch6_2.11-1.12.0.jar

# OTHER JARS
RUN wget -P /opt/flink/lib/ https://repo1.maven.org/maven2/org/apache/hive/hive-exec/3.1.2/hive-exec-3.1.2.jar
RUN wget -P /opt/flink/lib/ https://repo.maven.apache.org/maven2/mysql/mysql-connector-java/8.0.19/mysql-connector-java-8.0.19.jar
RUN wget -P /opt/flink/lib https://jdbc.postgresql.org/download/postgresql-42.2.19.jar

# Create data folders
RUN mkdir -p /opt/data; \
    mkdir -p /opt/data/stream; \
    echo "taskmanager.memory.jvm-metaspace.size: 512m" >> /opt/flink/conf/flink-conf.yaml; \
    echo "rest.port: 8088" >> /opt/flink/conf/flink-conf.yaml;

# Install hbase and run
RUN wget -P /opt/ https://downloads.apache.org/hbase/1.4.13/hbase-1.4.13-bin.tar.gz;
RUN tar zxvf /opt/hbase-1.4.13-bin.tar.gz -C /opt
RUN echo "export JAVA_HOME=/usr/local/openjdk-8" >> /opt/hbase-1.4.13/conf/hbase-env.sh
RUN echo "\n<configuration>\n  <property>\n    <name>hbase.tmp.dir</name>\n    <value>tmp</value>  </property>\n</configuration>" > /opt/hbase-1.4.13/conf/hbase-site.xml

RUN pip install jupyterlab jupyter-client

ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && apt install curl git cmake ack g++ python3-dev vim-youcompleteme tmux -yq
RUN apt-get install g++-8 -yq
RUN update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8
#RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/qooba/vim-python-ide/master/setup.sh)"

WORKDIR /opt/flink
RUN apt update && apt install python3-psycopg2 default-mysql-client mariadb-client -yq
RUN pip install kafka-python sqlalchemy PyMySQL scikit-learn mlflow nbdev matplotlib
CMD ["sh","-c", "jupyter lab --notebook-dir=/opt/flink/notebooks --ip='0.0.0.0' --port=8888 --no-browser --allow-root --NotebookApp.password='' --NotebookApp.token=''"]`
