package ru.yandex.practicum.telemetry.collector.service.hub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.DeviceRemovedEventProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.kafka.telemetry.event.DeviceRemovedEventAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

@Service
public class DeviceRemovedHubEventHandler extends BaseHubEventHandler<DeviceRemovedEventAvro> {

    public DeviceRemovedHubEventHandler(KafkaEventProducer kafkaEventProducer,
                                        @Value("${kafka.topic.hub}") String topic) {
        super(kafkaEventProducer, topic);
    }

    @Override
    protected DeviceRemovedEventAvro mapToAvro(HubEventProto hubEvent) {
        DeviceRemovedEventProto event = hubEvent.getDeviceRemoved();
        return DeviceRemovedEventAvro.newBuilder()
                .setId(event.getId())
                .build();
    }

    @Override
    public HubEventProto.PayloadCase getMessageType() {
        return HubEventProto.PayloadCase.DEVICE_REMOVED;
    }
}
