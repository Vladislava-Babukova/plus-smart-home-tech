package ru.yandex.practicum.telemetry.collector.service.sensor;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.telemetry.collector.model.sensor.SensorEvent;
import ru.yandex.practicum.telemetry.collector.service.KafkaEventProducer;

@Slf4j
@RequiredArgsConstructor
public abstract class BaseSensorEventHandler<T extends SpecificRecordBase> implements SensorEventHandler {

    protected final KafkaEventProducer kafkaEventProducer;
    protected final String topic;

    @Override
    public void handle(SensorEvent sensorEvent) {
        try {
            Producer<String, SpecificRecordBase> producer = kafkaEventProducer.getProducer();
            T specificAvroEvent = mapToAvro(sensorEvent);
            SensorEventAvro avroEvent = SensorEventAvro.newBuilder()
                    .setId(sensorEvent.getId())
                    .setHubId(sensorEvent.getHubId())
                    .setTimestamp(sensorEvent.getTimestamp())
                    .setPayload(specificAvroEvent)
                    .build();
            log.info("Начата отправка сообщений {} в топик {}", avroEvent, topic);

            ProducerRecord<String, SpecificRecordBase> record = new ProducerRecord<>(topic, avroEvent);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Ошибка отправки сообщения в топик {}", topic, exception);
                } else {
                    log.info("Сообщение отправлено в топик {} partition {} offset {}",
                            topic, metadata.partition(), metadata.offset());
                }
            });
            producer.flush();
        } catch (Exception e) {
            log.error("Ошибка обработки события", e);
        }
    }

    protected abstract T mapToAvro(SensorEvent sensorEvent);
}
