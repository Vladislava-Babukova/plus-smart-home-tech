package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.dal.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.dal.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.dal.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.dal.repository.ScenarioRepository;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@Transactional(readOnly = true)
@RequiredArgsConstructor
public class SnapshotAnalyzer {
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;

    public List<Scenario> analyze(String hubId, SensorsSnapshotAvro sensorsSnapshotAvro) {
        List<Scenario> allScenarios = scenarioRepository.findByHubId(hubId);
        List<Scenario> triggeredScenarios = new ArrayList<>();

        for (Scenario scenario : allScenarios) {
            if (checkScenarioConditions(scenario, sensorsSnapshotAvro)) {
                triggeredScenarios.add(scenario);
                log.info("Сценарий '{}' для хаба {} активирован", scenario.getName(), hubId);
            }
        }
        return triggeredScenarios;
    }

    private boolean checkScenarioConditions(Scenario scenario, SensorsSnapshotAvro snapshot) {
        for (Map.Entry<String, Long> entry : scenario.getConditionIds().entrySet()) {
            String sensorId = entry.getKey();
            Long conditionId = entry.getValue();
            Condition condition = conditionRepository.findById(conditionId).orElse(null);
            if (condition == null || !checkCondition(sensorId, condition, snapshot)) {
                return false;
            }
        }
        return true;
    }

    private boolean checkCondition(String sensorId, Condition condition, SensorsSnapshotAvro snapshot) {
        SensorStateAvro sensorState = findSensorInSnapshot(sensorId, snapshot);
        if (sensorState == null) {
            log.warn("Сенсор {} не найден в снапшоте", sensorId);
            return false;
        }
        Integer sensorValue = getSensorValue(sensorState, condition.getType());
        if (sensorValue == null) {
            log.warn("Не удалось получить значение для типа условия {} из сенсора {}", condition.getType(), sensorId);
            return false;
        }
        return checkConditionValue(sensorValue, condition.getOperation(), condition.getValue());
    }

    private SensorStateAvro findSensorInSnapshot(String sensorId, SensorsSnapshotAvro snapshot) {
        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();
        if (sensorsState == null) {
            return null;
        }
        return sensorsState.get(sensorId);
    }

    private Integer getSensorValue(SensorStateAvro sensorState, ConditionTypeAvro conditionType) {
        Object data = sensorState.getData();

        switch (conditionType) {
            case TEMPERATURE:
                if (data instanceof ClimateSensorAvro) {
                    return ((ClimateSensorAvro) data).getTemperatureC();
                } else if (data instanceof TemperatureSensorAvro) {
                    return ((TemperatureSensorAvro) data).getTemperatureC();
                }
                break;
            case HUMIDITY:
                if (data instanceof ClimateSensorAvro) {
                    return ((ClimateSensorAvro) data).getHumidity();
                }
                break;
            case CO2LEVEL:
                if (data instanceof ClimateSensorAvro) {
                    return ((ClimateSensorAvro) data).getCo2Level();
                }
                break;
            case LUMINOSITY:
                if (data instanceof LightSensorAvro) {
                    return ((LightSensorAvro) data).getLuminosity();
                }
                break;
            case MOTION:
                if (data instanceof MotionSensorAvro) {
                    return ((MotionSensorAvro) data).getMotion() ? 1 : 0;
                }
                break;
            case SWITCH:
                if (data instanceof SwitchSensorAvro) {
                    return ((SwitchSensorAvro) data).getState() ? 1 : 0;
                }
                break;
        }
        return null;
    }

    private boolean checkConditionValue(Integer sensorValue, ConditionOperationAvro operation, Integer conditionValue) {
        if (sensorValue == null || conditionValue == null) {
            return false;
        }
        switch (operation) {
            case EQUALS:
                return sensorValue.equals(conditionValue);
            case GREATER_THAN:
                return sensorValue > conditionValue;
            case LOWER_THAN:
                return sensorValue < conditionValue;
            default:
                log.warn("Неизвестная операция: {}", operation);
                return false;
        }
    }
}
