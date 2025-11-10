package ru.yandex.practicum.telemetry.collector.service.sensor;

import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEventType;

public interface SensorEventHandler {
    SensorEventType getMessageType();

    void handle(SensorEvent event);
}
