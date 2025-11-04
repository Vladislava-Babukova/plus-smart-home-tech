package ru.yandex.practicum.telemetry.collector.service.hub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.kafka.telemetry.event.DeviceAddedEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.DeviceTypeAvro;
import ru.yandex.practicum.telemetry.collector.model.hub.DeviceAddedEvent;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEvent;
import ru.yandex.practicum.telemetry.collector.model.hub.HubEventType;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

@Service
public class DeviceAddedHubEventHandler extends BaseHubEventHandler<DeviceAddedEventAvro> {

    public DeviceAddedHubEventHandler(KafkaEventProducer kafkaEventProducer,
                                      @Value("${kafka.topic.hub}") String topic) {
        super(kafkaEventProducer, topic);
    }

    @Override
    protected DeviceAddedEventAvro mapToAvro(HubEvent hubEvent) {
        DeviceAddedEvent event = (DeviceAddedEvent) hubEvent;
        return DeviceAddedEventAvro.newBuilder()
                .setId(event.getId())
                .setType(DeviceTypeAvro.valueOf(event.getDeviceType().name()))
                .build();
    }

    @Override
    public HubEventType getMessageType() {
        return HubEventType.DEVICE_ADDED;
    }
}
