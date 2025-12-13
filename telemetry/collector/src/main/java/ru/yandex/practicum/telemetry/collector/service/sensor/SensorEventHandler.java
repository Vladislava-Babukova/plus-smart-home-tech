package ru.yandex.practicum.telemetry.collector.service.sensor;

import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

public interface SensorEventHandler {
    SensorEventProto.PayloadCase getMessageType();

    void handle(SensorEventProto event);
}
