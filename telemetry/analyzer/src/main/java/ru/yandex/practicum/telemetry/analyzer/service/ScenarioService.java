package ru.yandex.practicum.telemetry.analyzer.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.dal.model.Action;
import ru.yandex.practicum.telemetry.analyzer.dal.model.Condition;
import ru.yandex.practicum.telemetry.analyzer.dal.model.Scenario;
import ru.yandex.practicum.telemetry.analyzer.dal.model.Sensor;
import ru.yandex.practicum.telemetry.analyzer.dal.repository.ActionRepository;
import ru.yandex.practicum.telemetry.analyzer.dal.repository.ConditionRepository;
import ru.yandex.practicum.telemetry.analyzer.dal.repository.ScenarioRepository;
import ru.yandex.practicum.telemetry.analyzer.dal.repository.SensorRepository;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class ScenarioService {

    private final ScenarioRepository scenarioRepository;
    private final SensorRepository sensorRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Transactional
    public void handleScenarioAdded(String hubId, ScenarioAddedEventAvro scenarioAddedEvent) {
        log.info("Обработка добавления сценария {} для хаба {}", scenarioAddedEvent.getName(), hubId);

        Optional<Scenario> existingScenario = scenarioRepository.findByHubIdAndName(hubId, scenarioAddedEvent.getName());
        if (existingScenario.isPresent()) {
            log.warn("Сценарий {} для хаба {} уже существует. Удаляем старый.", scenarioAddedEvent.getName(), hubId);
            scenarioRepository.delete(existingScenario.get());
        }

        Scenario scenario = new Scenario();
        scenario.setHubId(hubId);
        scenario.setName(scenarioAddedEvent.getName());

        Map<String, Condition> conditions = processConditions(hubId, scenarioAddedEvent.getConditions());
        scenario.setConditions(conditions);

        Map<String, Action> actions = processActions(hubId, scenarioAddedEvent.getActions());
        scenario.setActions(actions);

        scenarioRepository.save(scenario);
        log.info("Сценарий {} для хаба {} успешно сохранен", scenarioAddedEvent.getName(), hubId);
    }

    @Transactional
    public void handleScenarioRemoved(String hubId, ScenarioRemovedEventAvro scenarioRemovedEvent) {
        log.info("Обработка удаления сценария {} для хаба {}", scenarioRemovedEvent.getName(), hubId);

        Optional<Scenario> scenario = scenarioRepository.findByHubIdAndName(hubId, scenarioRemovedEvent.getName());
        if (scenario.isPresent()) {
            scenarioRepository.delete(scenario.get());
            log.info("Сценарий {} для хаба {} успешно удален", scenarioRemovedEvent.getName(), hubId);
        } else {
            log.warn("Сценарий {} для хаба {} не найден", scenarioRemovedEvent.getName(), hubId);
        }
    }

    @Transactional
    public void handleDeviceAdded(String hubId, DeviceAddedEventAvro deviceAddedEvent) {
        log.info("Обработка добавления устройства {} типа {} для хаба {}",
                deviceAddedEvent.getId(), deviceAddedEvent.getType(), hubId);

        Optional<Sensor> existingSensor = sensorRepository.findById(deviceAddedEvent.getId());
        if (existingSensor.isPresent()) {
            log.warn("Датчик {} уже существует", deviceAddedEvent.getId());
            return;
        }

        Sensor sensor = new Sensor();
        sensor.setId(deviceAddedEvent.getId());
        sensor.setHubId(hubId);
        sensorRepository.save(sensor);

        log.info("Датчик {} для хаба {} успешно сохранен", deviceAddedEvent.getId(), hubId);
    }

    @Transactional
    public void handleDeviceRemoved(String hubId, DeviceRemovedEventAvro deviceRemovedEvent) {
        log.info("Обработка удаления устройства {} для хаба {}", deviceRemovedEvent.getId(), hubId);

        Optional<Sensor> sensor = sensorRepository.findById(deviceRemovedEvent.getId());
        if (sensor.isPresent()) {
            if (!sensor.get().getHubId().equals(hubId)) {
                log.warn("Датчик {} не принадлежит хабу {}", deviceRemovedEvent.getId(), hubId);
                return;
            }

            sensorRepository.delete(sensor.get());
            log.info("Датчик {} для хаба {} успешно удален", deviceRemovedEvent.getId(), hubId);
        } else {
            log.warn("Датчик {} не найден", deviceRemovedEvent.getId());
        }
    }


    private Map<String, Condition> processConditions(String hubId, List<ScenarioConditionAvro> conditionsAvro) {
        Map<String, Condition> conditions = new HashMap<>();

        for (ScenarioConditionAvro conditionAvro : conditionsAvro) {
            Optional<Sensor> sensor = sensorRepository.findByIdAndHubId(conditionAvro.getSensorId(), hubId);
            if (sensor.isEmpty()) {
                log.warn("Датчик {} не найден для хаба {}. Пропускаем условие.", conditionAvro.getSensorId(), hubId);
                continue;
            }

            Condition condition = new Condition();
            condition.setType(conditionAvro.getType());
            condition.setOperation(conditionAvro.getOperation());

            Integer value = extractValue(conditionAvro.getValue());
            condition.setValue(value);

            Condition savedCondition = conditionRepository.save(condition);
            conditions.put(conditionAvro.getSensorId(), savedCondition);
        }

        return conditions;
    }

    private Map<String, Action> processActions(String hubId, List<DeviceActionAvro> actionsAvro) {
        Map<String, Action> actions = new HashMap<>();

        for (DeviceActionAvro actionAvro : actionsAvro) {
            Optional<Sensor> sensor = sensorRepository.findByIdAndHubId(actionAvro.getSensorId(), hubId);
            if (sensor.isEmpty()) {
                log.warn("Датчик {} не найден для хаба {}. Пропускаем действие.", actionAvro.getSensorId(), hubId);
                continue;
            }

            Action action = new Action();
            action.setType(actionAvro.getType());

            Integer value = extractValue(actionAvro.getValue());
            action.setValue(value);

            Action savedAction = actionRepository.save(action);
            actions.put(actionAvro.getSensorId(), savedAction);
        }
        return actions;
    }

    private Integer extractValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? 1 : 0;
        }
        return null;
    }
}
