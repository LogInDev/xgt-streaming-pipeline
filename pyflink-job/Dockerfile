FROM apache/flink:1.19.0-scala_2.12-java11

RUN apt-get update && apt-get install -y \
    python3.11 python3.11-venv python3.11-dev curl && \
    curl -sS https://bootstrap.pypa.io/get-pip.py | python3.11 && \
    python3.11 -m venv /opt/venv

ENV PATH="/opt/venv/bin:$PATH"

# Kafka Connector JAR 및 JAR 적용 설정파일 복사
COPY lib/*.jar /opt/flink/lib/
COPY conf/flink-conf.yaml /opt/flink/conf/

# ✅ PyFlink는 공식 빌드된 안정 버전 설치
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

WORKDIR /opt/job
COPY sensor_stream.py .

CMD ["python", "sensor_stream.py"]