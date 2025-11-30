package ru.yandex.practicum.telemetry.aggregator.service;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.aggregator.config.KafkaAggregatorConfig;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Component
public class AggregationStarter implements ApplicationListener<ContextRefreshedEvent> {

    private final SnapshotService snapshotService;
    private final KafkaAggregatorConfig kafkaConfig;

    private KafkaConsumer<String, SensorEventAvro> consumer;
    private KafkaProducer<String, SensorsSnapshotAvro> producer;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new ConcurrentHashMap<>();
    private volatile boolean running = true;

    public AggregationStarter(SnapshotService snapshotService, KafkaAggregatorConfig kafkaConfig) {
        this.snapshotService = snapshotService;
        this.kafkaConfig = kafkaConfig;
    }

    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        log.info("Application context refreshed, starting aggregation process...");
        initializeKafkaClients();
        startAggregation();
    }

    @PreDestroy
    public void preDestroy() {
        log.info("Shutdown signal received, stopping aggregation...");
        running = false;
        if (consumer != null) {
            consumer.wakeup();
        }
    }

    private void initializeKafkaClients() {
        log.info("Initializing Kafka clients...");
        this.consumer = new KafkaConsumer<>(kafkaConfig.getConsumer().getProperties());
        this.producer = new KafkaProducer<>(kafkaConfig.getProducer().getProperties());

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("JVM shutdown hook triggered");
            running = false;
            if (consumer != null) {
                consumer.wakeup();
            }
        }));
    }

    public void startAggregation() {
        try {
            String topic = kafkaConfig.getConsumer().getTopic();
            consumer.subscribe(List.of(topic));
            log.info("Subscribed to topic: {}", topic);

            while (running) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(
                        kafkaConfig.getConsumer().getPollTimeout()
                );

                if (!records.isEmpty()) {
                    processRecordsBatch(records);
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer wakeup called");
        } catch (Exception e) {
            log.error("Error during sensor events processing", e);
        } finally {
            shutdown();
        }
    }

    private void processRecordsBatch(ConsumerRecords<String, SensorEventAvro> records) {
        int processedCount = 0;

        for (ConsumerRecord<String, SensorEventAvro> record : records) {
            if (!running) {
                break;
            }

            log.trace("Processing message from hub {} from partition {} with offset {}",
                    record.key(), record.partition(), record.offset());

            processSensorEvent(record.value());
            updateCurrentOffset(record);
            processedCount++;

            if (processedCount % 100 == 0) {
                commitOffsetsAsync();
            }
        }

        if (processedCount > 0) {
            commitOffsetsAsync();
            producer.flush();
        }
    }

    private void updateCurrentOffset(ConsumerRecord<String, SensorEventAvro> record) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1, "Processed at: " + System.currentTimeMillis())
        );
    }

    private void commitOffsetsAsync() {
        if (!currentOffsets.isEmpty()) {
            consumer.commitAsync(currentOffsets, this::handleCommitCallback);
        }
    }

    private void handleCommitCallback(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            log.warn("Error committing offsets for partitions: {}", offsets.keySet(), exception);
        } else {
            log.debug("Successfully committed offsets for {} partitions", offsets.size());
        }
    }

    private void processSensorEvent(SensorEventAvro sensorEvent) {
        try {
            Optional<SensorsSnapshotAvro> snapshotOptional = snapshotService.updateState(sensorEvent);

            if (snapshotOptional.isPresent()) {
                sendSnapshot(snapshotOptional.get());
            } else {
                log.debug("Snapshot not updated for hub: {}, sensor: {}",
                        sensorEvent.getHubId(), sensorEvent.getId());
            }
        } catch (Exception e) {
            log.error("Error processing sensor event for hub: {}, sensor: {}",
                    sensorEvent.getHubId(), sensorEvent.getId(), e);
        }
    }

    private void sendSnapshot(SensorsSnapshotAvro snapshot) {
        ProducerRecord<String, SensorsSnapshotAvro> record = new ProducerRecord<>(
                kafkaConfig.getProducer().getTopic(),
                null,
                snapshot.getTimestamp().toEpochMilli(),
                snapshot.getHubId(),
                snapshot
        );

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Error sending snapshot to topic {} for hub {}",
                        kafkaConfig.getProducer().getTopic(), snapshot.getHubId(), exception);
            } else {
                log.info("Snapshot sent successfully to topic {} partition {} offset {} for hub {}",
                        metadata.topic(), metadata.partition(), metadata.offset(), snapshot.getHubId());
            }
        });
    }

    private void shutdown() {
        log.info("Starting graceful shutdown...");
        try {
            if (!currentOffsets.isEmpty()) {
                consumer.commitSync(currentOffsets);
                log.info("Final offsets committed for {} partitions", currentOffsets.size());
            }
        } catch (Exception e) {
            log.error("Error during final offset commit", e);
        } finally {
            closeResources();
        }
    }

    private void closeResources() {
        try {
            producer.flush();
            log.info("Producer flushed successfully");
        } catch (Exception e) {
            log.error("Error flushing producer", e);
        }

        try {
            consumer.close();
            log.info("Consumer closed successfully");
        } catch (Exception e) {
            log.error("Error closing consumer", e);
        }

        try {
            producer.close();
            log.info("Producer closed successfully");
        } catch (Exception e) {
            log.error("Error closing producer", e);
        }

        log.info("Aggregation service shutdown completed");
    }
}