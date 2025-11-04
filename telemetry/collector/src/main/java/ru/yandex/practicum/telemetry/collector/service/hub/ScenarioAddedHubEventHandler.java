package ru.yandex.practicum.telemetry.collector.service.hub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.ScenarioAddedEventAvro;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEventType;
import ru.yandex.practicum.telemetry.collector.model.hub.ScenarioAddedEvent;
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
    protected ScenarioAddedEventAvro mapToAvro(HubEvent hubEvent) {
        ScenarioAddedEvent event = (ScenarioAddedEvent) hubEvent;
        return ScenarioAddedEventAvro.newBuilder()
                .setName(event.getName())
                .setConditions(mapConditionsToAvro(event.getConditions()))
                .setActions(mapActionsToAvro(event.getActions()))
                .build();
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.SCENARIO_ADDED;
    }
}
