package ru.yandex.practicum.telemetry.collector.model.hub;

import lombok.Builder;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@Builder
public class ScenarioCondition {
    private String sensorId;
    private ScenarioConditionType type;
    private ScenarioConditionOperation operation;
    private Integer value;
}
