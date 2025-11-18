package ru.yandex.practicum.telemetry.collector.service.hub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioAddedEventProto;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

import static ru.yandex.practicum.telemetry.collector.service.hub.mapper.ClassToAvroMapper.mapActionsToAvro;
import static ru.yandex.practicum.telemetry.collector.service.hub.mapper.ClassToAvroMapper.mapConditionsToAvro;

@Service
public class ScenarioAddedHubEventHandler extends BaseHubEventHandler<ScenarioAddedEventAvro> {

    public ScenarioAddedHubEventHandler(KafkaEventProducer kafkaEventProducer,
                                        @Value("${kafka.topic.hub}") String topic) {
        super(kafkaEventProducer, topic);
    }

    @Override
    protected ScenarioAddedEventAvro mapToAvro(HubEventProto hubEvent) {
        ScenarioAddedEventProto event = hubEvent.getScenarioAdded();
        return ScenarioAddedEventAvro.newBuilder()
                .setName(event.getName())
                .setConditions(mapConditionsToAvro(event.getConditionList()))
                .setActions(mapActionsToAvro(event.getActionList()))
                .build();
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.SCENARIO_ADDED;
    }
}