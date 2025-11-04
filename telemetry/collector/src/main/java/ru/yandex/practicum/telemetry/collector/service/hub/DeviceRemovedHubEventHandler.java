package ru.yandex.practicum.telemetry.collector.service.hub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.telemetry.collector.model.hub.DeviceRemovedEvent;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEventType;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

@Service
public class DeviceRemovedHubEventHandler extends BaseHubEventHandler<DeviceRemovedEventAvro> {

    public DeviceRemovedHubEventHandler(KafkaEventProducer kafkaEventProducer,
                                        @Value("${kafka.topic.hub}") String topic) {
        super(kafkaEventProducer, topic);
    }

    @Override
    protected DeviceRemovedEventAvro mapToAvro(HubEvent hubEvent) {
        DeviceRemovedEvent event = (DeviceRemovedEvent) hubEvent;
        return DeviceRemovedEventAvro.newBuilder()
                .setId(event.getId())
                .build();
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.DEVICE_REMOVED;
    }
}
