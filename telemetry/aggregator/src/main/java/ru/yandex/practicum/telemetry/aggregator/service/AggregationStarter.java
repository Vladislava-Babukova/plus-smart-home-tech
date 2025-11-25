package ru.yandex.practicum.telemetry.aggregator.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.config.KafkaAggregatorConfig;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
@RequiredArgsConstructor
public class AggregationStarter {
    private final SnapshotService snapshotService;

    private static final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

    private final KafkaAggregatorConfig.ConsumerConfig consumerConfig;
    private final KafkaConsumer<String, SensorEventAvro> consumer;

    private final KafkaAggregatorConfig.ProducerConfig producerConfig;
    private final KafkaProducer<String, SensorsSnapshotAvro> producer;

    @Autowired
    public AggregationStarter(SnapshotService snapshotService, KafkaAggregatorConfig kafkaConfig) {
        this.snapshotService = snapshotService;
        this.consumerConfig = kafkaConfig.getConsumer();
        this.producerConfig = kafkaConfig.getProducer();

        this.consumer = new KafkaConsumer<>(consumerConfig.getProperties());
        this.producer = new KafkaProducer<>(producerConfig.getProperties());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Сработал хук на завершение JVM. Прерываю работу консьюмера.");
            consumer.wakeup();
        }));
    }

    public void start() {
        try {
            consumer.subscribe(List.of(consumerConfig.getTopic()));

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(consumerConfig.getPollTimeout());
                int count = 0;
                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    log.trace("Обработка сообщения от хаба {} из партиции {} с офсетом {}.",
                            record.key(), record.partition(), record.offset());
                    handleRecord(record.value());
                    manageOffsets(record, count, consumer);
                    count++;
                }
                producer.flush();
                consumer.commitAsync();
            }
        } catch (WakeupException ignores) {
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                producer.flush();
                consumer.commitSync(currentOffsets);

            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
            }
        }
    }

    private static void manageOffsets(ConsumerRecord<String, SensorEventAvro> record, int count,
                                      KafkaConsumer<String, SensorEventAvro> consumer) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        );

        if(count % 100 == 0) {
            consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                if(exception != null) {
                    log.warn("Ошибка во время фиксации оффсетов: {}", offsets, exception);
                }
            });
        }
    }

    private void handleRecord(SensorEventAvro sensorEventAvro) {
        Optional<SensorsSnapshotAvro> newSnapshot = snapshotService.updateState(sensorEventAvro);
        if(newSnapshot.isPresent()) {
            SensorsSnapshotAvro snapshot = newSnapshot.get();
            try {
                log.info("Начинаю отправку сообщений {} в топик {}", snapshot, producerConfig.getTopic());
                ProducerRecord<String, SensorsSnapshotAvro> record = new ProducerRecord<>(producerConfig.getTopic()
                        , null, snapshot.getTimestamp().toEpochMilli(), snapshot.getHubId(), snapshot);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        log.error("Ошибка отправки сообщения в топик {}", producerConfig.getTopic(), exception);
                    } else {
                        log.info("Сообщение отправлено в топик {} partition {} offset {}",
                                producerConfig.getTopic(), metadata.partition(), metadata.offset());
                    }
                });
                producer.flush();
            } catch (Exception e) {
                log.error("Ошибка обработки события", e);
            }
        } else {
            log.info("Снепшот {} не обновился", sensorEventAvro);
        }
    }
}