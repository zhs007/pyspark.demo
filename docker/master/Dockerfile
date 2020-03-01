FROM bde2020/spark-master:2.4.5-hadoop2.7

MAINTAINER zerro "zerrozhao@gmail.com"

RUN apk add make automake gcc g++ python3-dev \
    && rm -rf /var/lib/apt/lists/*

COPY *.jar /spark/jars/