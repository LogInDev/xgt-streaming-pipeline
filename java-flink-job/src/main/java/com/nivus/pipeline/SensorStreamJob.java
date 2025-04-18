package com.nivus.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SensorStreamJob {
    public static void main(String[] args) throws Exception {
        TagRuleManager.loadFromResource("tag_rules.json");

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setTopics("raw_data")
                .setGroupId("flink-sensor-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // JSON 파싱 + 이상치 판단
        DataStream<EnrichedSensorData> processedStream = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .map(new SensorDataProcessor());

        // 이상치만 필터링
        DataStream<EnrichedSensorData> outlierStream = processedStream
                .filter(EnrichedSensorData::isOutlier);

        // Sink #1: 이상치만 보내는 KafkaSink
        KafkaSink<String> outlierSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("outlier_data")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // Sink #2: 전체 데이터를 enriched_data 토픽으로
        KafkaSink<String> enrichedSink = KafkaSink.<String>builder()
                .setBootstrapServers("kafka:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("enriched_data")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // 결과 sink로 전송
        outlierStream.map(SensorStreamJob::toJson).sinkTo(outlierSink);
        processedStream.map(SensorStreamJob::toJson).sinkTo(enrichedSink);

        env.execute("Sensor Outlier Stream Job");
    }

    // JSON 직렬화
    private static String toJson(EnrichedSensorData data) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
            return mapper.writeValueAsString(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}