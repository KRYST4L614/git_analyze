FROM apache/spark:4.1.0-preview3

USER root

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    curl \
    wget \
    netcat-openbsd \
    iputils-ping \
    zip \
    unzip \
    && rm -rf /var/lib/apt/lists/*

RUN ln -s /usr/bin/python3 /usr/bin/python

RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/postgresql-42.6.0.jar \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

WORKDIR /app

COPY ../../../ .

RUN pip3 install --no-cache-dir -r requirements.txt

RUN mkdir -p /tmp/pure_python_deps
RUN pip3 install --target /tmp/pure_python_deps --no-cache-dir geopy cachetools requests

RUN cp -r ./src /tmp/pure_python_deps/
RUN cp -r ./dependencies /tmp/pure_python_deps/

RUN cd /tmp/pure_python_deps && zip -r /opt/spark/pure_deps.zip .

ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYTHONPATH=/app:$PYTHONPATH

RUN mkdir -p /tmp/spark-warehouse /tmp/spark-local
