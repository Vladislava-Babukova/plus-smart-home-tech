package ru.yandex.practicum.telemetry.collector.controller;

import jakarta.validation.Valid;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEventType;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEventType;
import ru.yandex.practicum.telemetry.collector.service.hub.HubEventHandler;
import ru.yandex.practicum.telemetry.collector.service.sensor.SensorEventHandler;

import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@RestController
@Validated
@RequestMapping("/events")
public class EventController {

    private final Map<SensorEventType, SensorEventHandler> sensorEventHandlers;
    private final Map<HubEventType, HubEventHandler> hubEventHandlers;

    public EventController(Set<SensorEventHandler> sensorEventHandlers, Set<HubEventHandler> hubEventHandlers) {
        this.sensorEventHandlers = sensorEventHandlers.stream()
                .collect(Collectors.toMap(SensorEventHandler::getMessageType, Function.identity()));
        this.hubEventHandlers = hubEventHandlers.stream()
                .collect(Collectors.toMap(HubEventHandler::getMessageType, Function.identity()));
    }

    @PostMapping("/sensors")
    public void collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("Sensor json: {}", event.toString());
        SensorEventHandler sensorEventHandler = sensorEventHandlers.get(event.getType());
        if (sensorEventHandler == null) {
            throw new IllegalArgumentException("Подходящий обработчик для события датчика " + event.getType() +
                    " не найден");
        }
        sensorEventHandler.handle(event);
    }

    @PostMapping("/hubs")
    public void collectHubEvent(@Valid @RequestBody HubEvent event) {
        log.info("Hub json: {}", event.toString());
        HubEventHandler hubEventHandler = hubEventHandlers.get(event.getType());
        if (hubEventHandler == null) {
            throw new IllegalArgumentException("Подходящий обработчик для события хаба " + event.getType() +
                    " не найден");
        }
        hubEventHandler.handle(event);
    }
}