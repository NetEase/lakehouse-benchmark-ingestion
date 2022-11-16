FROM openjdk:8u332-jdk
RUN apt update \
    && apt-get install -y netcat \
    && apt-get install -y vim \
    && apt-get install -y net-tools \
    && apt-get install -y telnet

# RUN cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && echo 'Asia/Shanghai' >/etc/timezone

COPY arctic_benchmark_ingestion.tar.gz /usr/lib/benchmark-ingestion/arctic_benchmark_ingestion.tar.gz
RUN cd /usr/lib/benchmark-ingestion &&  tar -zxvf arctic_benchmark_ingestion.tar.gz && rm -rf arctic_benchmark_ingestion.tar.gz
WORKDIR /usr/lib/benchmark-ingestion
