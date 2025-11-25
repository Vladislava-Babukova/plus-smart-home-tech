package ru.yandex.practicum.telemetry.analyzer.service;

import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.dal.model.Scenario;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotProcessor {

    private final SnapshotAnalyzer snapshotAnalyzer;
    private final GrpcClientService grpcClientService;
    private final KafkaConsumer<String, SensorsSnapshotAvro> consumer;

    @EventListener(ApplicationReadyEvent.class)
    public void start() {
        CompletableFuture.runAsync(() -> {
            Thread.currentThread().setName("SnapshotProcessorThread");

            try {
                consumer.subscribe(List.of("telemetry.snapshots.v1"));
                Map<TopicPartition, OffsetAndMetadata> currentOffsets = new HashMap<>();

                while (true) {
                    ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(500));
                    int count = 0;

                    for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                        log.trace("Обработка сообщения от хаба {} из партиции {} с офсетом {}.",
                                record.key(), record.partition(), record.offset());
                        handleRecord(record.value());
                        manageOffsets(record, count, consumer, currentOffsets);
                        count++;
                    }
                    consumer.commitAsync();
                }
            } catch (WakeupException e) {
                log.info("Получен сигнал завершения работы");
            } catch (Exception e) {
                log.error("Ошибка во время обработки событий от хабов", e);
            } finally {
                consumer.close();
            }
        });
    }

    @PreDestroy
    public void shutdown() {
        consumer.wakeup();
    }

    private void manageOffsets(ConsumerRecord<String, SensorsSnapshotAvro> record, int count,
                               KafkaConsumer<String, SensorsSnapshotAvro> consumer,
                               Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        currentOffsets.put(
                new TopicPartition(record.topic(), record.partition()),
                new OffsetAndMetadata(record.offset() + 1)
        );

        if (count % 100 == 0) {
            consumer.commitAsync(currentOffsets, this::handleCommitCallback);
        }
    }

    private void handleCommitCallback(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
        if (exception != null) {
            log.warn("Ошибка во время фиксации оффсетов: {}", offsets, exception);
        }
    }

    @Transactional
    private void handleRecord(SensorsSnapshotAvro sensorsSnapshotAvro) {
        try {
            String hubId = sensorsSnapshotAvro.getHubId();
            List<Scenario> scenarios = snapshotAnalyzer.analyze(hubId, sensorsSnapshotAvro);
            for (Scenario scenario : scenarios) {
                grpcClientService.handleScenario(scenario);
            }
        } catch (Exception e) {
            log.error("Ошибка обработки события {}", sensorsSnapshotAvro, e);
        }
    }
}