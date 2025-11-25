package ru.yandex.practicum.telemetry.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc;
import ru.yandex.practicum.kafka.telemetry.event.ActionTypeAvro;
import ru.yandex.practicum.telemetry.analyzer.dal.model.Action;
import ru.yandex.practicum.telemetry.analyzer.dal.model.Scenario;

import java.time.Instant;
import java.util.Map;

@Slf4j
@Service
public class GrpcClientService {
    private final HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient;

    public GrpcClientService(@GrpcClient("hub-router") HubRouterControllerGrpc.HubRouterControllerBlockingStub hubRouterClient) {
        this.hubRouterClient = hubRouterClient;
    }

    public void handleScenario(Scenario scenario) {
        log.trace("Поступил сценарий {} для отправки хабу {}", scenario, scenario.getHubId());
        Instant time = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(time.getEpochSecond())
                .setNanos(time.getNano())
                .build();
        for (Map.Entry<String, Action> actionEntry : scenario.getActions().entrySet()) {
            Action scenarioAction = actionEntry.getValue();
            DeviceActionProto.Builder acctionBuilder = DeviceActionProto.newBuilder()
                    .setSensorId(actionEntry.getKey())
                    .setType(ActionTypeProto.valueOf(scenarioAction.getType().name()));
            if (scenarioAction.getType().equals(ActionTypeAvro.SET_VALUE)) {
                acctionBuilder.setValue(scenarioAction.getValue());
            }
            try {
                log.info("Отправка сообщения хабу");
                hubRouterClient.handleDeviceAction(DeviceActionRequest.newBuilder()
                        .setHubId(scenario.getHubId())
                        .setScenarioName(scenario.getName())
                        .setAction(acctionBuilder.build())
                        .setTimestamp(timestamp)
                        .build());
            } catch (Exception e) {
                log.error("Ошибка при отправке хабу {} действия {} для устройства {}", scenario.getHubId(),
                        scenarioAction, scenarioAction.getId(), e);
            }
        }
    }
}
