FROM apache/flink:1.19.2-java11

# Python 설치 + pip 업그레이드
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install --upgrade pip setuptools wheel && \
    ln -s /usr/bin/python3 /usr/bin/python

COPY requirements.txt .
RUN pip3 install --no-cache-dir -r requirements.txt


# Kafka 커넥터 JAR 복사
COPY lib/*.jar /opt/flink/lib/

WORKDIR /opt/pyflink
COPY pyflink_job.py .

# pyflink는 pip 설치 필요 없음 (flink run -py로 실행)
