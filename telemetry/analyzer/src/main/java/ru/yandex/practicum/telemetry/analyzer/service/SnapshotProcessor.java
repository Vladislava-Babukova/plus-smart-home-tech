package ru.yandex.practicum.telemetry.analyzer.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaConfig;
import ru.yandex.practicum.telemetry.analyzer.dal.model.Scenario;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;

@Slf4j
@Component
public class SnapshotProcessor implements DisposableBean {

    private final KafkaConsumer<String, SensorsSnapshotAvro> consumer;
    private final List<String> topics;
    private final Duration pollTimeout;
    private final SnapshotAnalyzer snapshotAnalyzer;
    private final GrpcClientService grpcClientService;

    private volatile boolean running = false;
    private Future<?> consumerFuture;

    public SnapshotProcessor(KafkaConfig config,
                             SnapshotAnalyzer snapshotAnalyzer,
                             GrpcClientService grpcClientService) {

        KafkaConfig.ConsumerConfig consumerConfig = config.getConsumers().get("SnapshotProcessor");

        this.consumer = new KafkaConsumer<>(consumerConfig.getProperties());
        this.topics = consumerConfig.getTopics();
        this.pollTimeout = consumerConfig.getPollTimeout();
        this.snapshotAnalyzer = snapshotAnalyzer;
        this.grpcClientService = grpcClientService;
    }

    @PostConstruct
    public void start() {
        log.info("Запуск SnapshotProcessor для топика: {}", topics);
        this.running = true;
        this.consumerFuture = CompletableFuture.runAsync(this::processMessages);
    }

    private void processMessages() {
        try {
            log.info("Подписываемся на топик: {}", topics);
            consumer.subscribe(topics);

            while (running) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(pollTimeout);

                if (!records.isEmpty()) {
                    log.debug("Получено {} снапшотов", records.count());
                    processRecords(records);
                }
            }
        } catch (WakeupException e) {
            log.info("Получен сигнал WakeupException. Завершаем работу");
        } catch (Exception e) {
            log.error("Ошибка во время обработки снапшотов", e);
        } finally {
            shutdown();
        }
    }

    private void processRecords(ConsumerRecords<String, SensorsSnapshotAvro> records) {
        for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
            try {
                log.trace("Обработка снапшота от хаба {} (partition: {}, offset: {})",
                        record.key(), record.partition(), record.offset());

                SensorsSnapshotAvro snapshot = record.value();
                String hubId = snapshot.getHubId();

                List<Scenario> scenarios = snapshotAnalyzer.analyze(hubId, snapshot);

                for (Scenario scenario : scenarios) {
                    grpcClientService.handleScenario(scenario);
                }

            } catch (Exception e) {
                log.error("Ошибка обработки снапшота (hub: {}, partition: {}, offset: {})",
                        record.key(), record.partition(), record.offset(), e);
            }
        }
        commitOffsetsAsync();
    }

    private void commitOffsetsAsync() {
        try {
            consumer.commitAsync((offsets, exception) -> {
                if (exception != null) {
                    log.warn("Ошибка при коммите оффсетов", exception);
                } else {
                    log.trace("Оффсеты успешно закоммичены");
                }
            });
        } catch (Exception e) {
            log.warn("Ошибка при асинхронном коммите", e);
        }
    }

    private void shutdown() {
        log.info("Завершение работы SnapshotProcessor...");
        try {
            consumer.commitSync();
            log.info("Финальные оффсеты закоммичены");
        } catch (Exception e) {
            log.error("Ошибка при финальном коммите оффсетов", e);
        } finally {
            consumer.close();
            log.info("Kafka consumer закрыт");
        }
    }

    @PreDestroy
    @Override
    public void destroy() {
        log.info("Остановка SnapshotProcessor...");
        this.running = false;
        if (consumerFuture != null) {
            consumerFuture.cancel(true);
        }
        consumer.wakeup();
    }
}