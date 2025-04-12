from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaSink, KafkaRecordSerializationSchema
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.typeinfo import Types
from pyflink.common.watermark_strategy import WatermarkStrategy
import json

def map_function(value):
    try:
        print(f"[RAW] {value}")
        data = json.loads(value)
        data['value'] = round(data['value'] * 1.5, 2)
        print(f"[PROCESSED] {data}")
        return json.dumps(data)
    except Exception as e:
        print(f"[ERROR] Invalid input: {value} / {e}")
        return None

env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)

source = KafkaSource.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_group_id("flink-consumer") \
    .set_topics("raw_data") \
    .set_value_only_deserializer(SimpleStringSchema()) \
    .build()

sink = KafkaSink.builder() \
    .set_bootstrap_servers("kafka:9092") \
    .set_record_serializer(KafkaRecordSerializationSchema.builder() \
        .set_topic("processed_data") \
        .set_value_serialization_schema(SimpleStringSchema()) \
        .build()) \
    .build()

env.from_source(source, watermark_strategy=WatermarkStrategy.no_watermarks(), source_name="Kafka Source") \
    .map(map_function, output_type=Types.STRING()) \
    .sink_to(sink)

env.execute("PyFlink Kafka Stream Processing")
print("✅ Flink job started successfully")
