FROM openjdk:8u332-jdk
RUN apt update \
    && apt-get install -y netcat \
    && apt-get install -y vim \
    && apt-get install -y net-tools \
    && apt-get install -y telnet \
    && apt-get install -y wget

# RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' >/etc/timezone
WORKDIR /usr/lib/benchmark-ingestion
RUN wget https://github.com/NetEase/lakehouse-benchmark-ingestion/releases/download/1.0/lakehouse_benchmark_ingestion.tar.gz
RUN tar -zxvf lakehouse_benchmark_ingestion.tar.gz && rm -rf lakehouse_benchmark_ingestion.tar.gz