package com.nivus.pipeline;

public class SensorDataProcessor implements MapFunction<String, EnrichedSensorData> {
    private static final ObjectMapper mapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public EnrichedSensorData map(String value) throws Exception {
        SensorData rawData = mapper.readValue(value, SensorData.class);

        boolean isOutlier = OutlierDetector.isOutlier(rawData);  // 외부 정책 또는 threshold 기반
        return new EnrichedSensorData(rawData, isOutlier);
    }
}
