package com.nivus.pipeline;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class SensorData {
    private String tagId;
    private LocalDateTime measDtm;
    private double dataValue;
    // 기타 필드
}
