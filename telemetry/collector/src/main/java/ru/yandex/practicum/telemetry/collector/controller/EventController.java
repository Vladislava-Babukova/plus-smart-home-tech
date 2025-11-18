package ru.yandex.practicum.telemetry.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.server.service.GrpcService;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.EventService;
@GrpcService
@Slf4j
@RestController
@Validated
@RequiredArgsConstructor
@RequestMapping("/events")
public class EventController {

    private final EventService eventService;

    @PostMapping("/sensors")
    public void collectSensorEvent(@Valid @RequestBody SensorEvent event) {
        log.info("получено событие датчика, type={}", event.getType());
        log.debug("SensorEvent = {}", event);
        eventService.processSensorEvent(event);
    }

    @PostMapping("/hubs")
    public void collectHubEvent(@Valid @RequestBody HubEvent event) {
        log.info("получено событие хаба, type={}", event.getType());
        log.debug("HubEvent = {}", event);
        eventService.processHubEvent(event);
    }
}