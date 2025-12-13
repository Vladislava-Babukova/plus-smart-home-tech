package ru.yandex.practicum.telemetry.collector.service.sensor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.grpc.telemetry.event.MotionSensorProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.MotionSensorAvro;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

@Service
public class MotionSensorEventHandler extends BaseSensorEventHandler<MotionSensorAvro> {

    public MotionSensorEventHandler(KafkaEventProducer kafkaEventProducer,
                                    @Value("${kafka.topic.sensor}") String topic) {
        super(kafkaEventProducer, topic);
    }

    @Override
    public SensorEventProto.PayloadCase getMessageType() {
        return SensorEventProto.PayloadCase.MOTION_SENSOR_PROTO;
    }

    @Override
    protected MotionSensorAvro mapToAvro(SensorEventProto sensorEvent) {
        MotionSensorProto event = sensorEvent.getMotionSensorProto();
        return MotionSensorAvro.newBuilder()
                .setLinkQuality(event.getLinkQuality())
                .setMotion(event.getMotion())
                .setVoltage(event.getVoltage())
                .build();
    }
}
