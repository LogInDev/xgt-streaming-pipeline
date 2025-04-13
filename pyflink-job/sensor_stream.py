from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
import json

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'pyflink-group'
    }

    # Kafka Consumer
    consumer = FlinkKafkaConsumer(
        topics='raw_data',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    # Kafka Producer
    producer = FlinkKafkaProducer(
        topic='processed_data',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'kafka:9092'}
    )

    # 데이터 처리 파이프라인 정의
    ds = env.add_source(consumer).map(
        lambda value: process(value), output_type=Types.STRING()
    )

    ds.add_sink(producer)

    env.execute("Kafka PyFlink Processing Job")

def process(value: str) -> str:
    """
    데이터 전처리/가공 로직을 정의
    """
    try:
        data = json.loads(value)
        # 간단한 예: value 필드를 2배로 증가
        data['data_value'] = float(data['data_value']) * 2
        return json.dumps(data)
    except Exception as e:
        return json.dumps({"error": str(e)})

if __name__ == '__main__':
    main()
