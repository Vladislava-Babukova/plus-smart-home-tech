package ru.yandex.practicum.telemetry.analyzer.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.*;
import ru.yandex.practicum.telemetry.analyzer.config.KafkaConfig;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;

@Slf4j
@Component
public class HubEventProcessor implements DisposableBean {

    private final KafkaConsumer<String, HubEventAvro> consumer;
    private final List<String> topics;
    private final Duration pollTimeout;
    private final ScenarioService scenarioService;

    private volatile boolean running = false;
    private Future<?> processorFuture;
    private final Map<TopicPartition, OffsetAndMetadata> currentOffsets = new ConcurrentHashMap<>();

    public HubEventProcessor(KafkaConfig config, ScenarioService scenarioService) {
        final KafkaConfig.ConsumerConfig consumerConfig = config.getConsumers().get("HubEventProcessor");

        Properties consumerProperties = new Properties();
        consumerProperties.putAll(consumerConfig.getProperties());

        this.consumer = new KafkaConsumer<>(consumerProperties);
        this.topics = consumerConfig.getTopics();
        this.pollTimeout = consumerConfig.getPollTimeout();
        this.scenarioService = scenarioService;
    }

    @PostConstruct
    public void start() {
        log.info("Запуск HubEventProcessor для топиков: {}", topics);
        this.running = true;

        this.processorFuture = CompletableFuture.runAsync(this::run);
    }

    public void run() {
        try {
            log.info("Подписываемся на топик: {}", topics);
            consumer.subscribe(topics);

            while (running && !Thread.currentThread().isInterrupted()) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(pollTimeout);
                int count = 0;

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    log.trace("Обработка события хаба {} из партиции {} с офсетом {}",
                            record.key(), record.partition(), record.offset());

                    boolean success = handleRecord(record.value());

                    if (success) {
                        currentOffsets.put(
                                new TopicPartition(record.topic(), record.partition()),
                                new OffsetAndMetadata(record.offset() + 1)
                        );
                        count++;

                        if (count % 50 == 0) {
                            commitOffsetsAsync();
                        }
                    }
                }
                if (count > 0) {
                    commitOffsetsAsync();
                }
            }
        } catch (WakeupException e) {
            log.info("Получен сигнал WakeupException. Завершаем работу HubEventProcessor");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий хабов", e);
        } finally {
            shutdown();
        }
    }

    private boolean handleRecord(HubEventAvro hubEventAvro) {
        try {
            String hubId = hubEventAvro.getHubId();
            switch (hubEventAvro.getPayload()) {
                case DeviceAddedEventAvro dae -> scenarioService.handleDeviceAdded(hubId, dae);
                case DeviceRemovedEventAvro dre -> scenarioService.handleDeviceRemoved(hubId, dre);
                case ScenarioAddedEventAvro sae -> scenarioService.handleScenarioAdded(hubId, sae);
                case ScenarioRemovedEventAvro sre -> scenarioService.handleScenarioRemoved(hubId, sre);
                default -> {
                    log.warn("Неизвестный тип события: {}", hubEventAvro);
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            log.error("Ошибка обработки события для хаба {}", hubEventAvro.getHubId(), e);
            return false;
        }
    }

    private void commitOffsetsAsync() {
        if (!currentOffsets.isEmpty()) {
            try {
                consumer.commitAsync(currentOffsets, (offsets, exception) -> {
                    if (exception != null) {
                        log.warn("Ошибка при фиксации оффсетов HubEventProcessor", exception);
                    }
                });
            } catch (Exception e) {
                log.warn("Ошибка при коммите оффсетов HubEventProcessor", e);
            }
        }
    }

    private void shutdown() {
        log.info("Завершение работы HubEventProcessor...");
        try {
            if (!currentOffsets.isEmpty()) {
                consumer.commitSync(currentOffsets);
                log.info("Последние оффсеты HubEventProcessor зафиксированы");
            }
        } catch (Exception e) {
            log.error("Ошибка при фиксации оффсетов при завершении", e);
        } finally {
            consumer.close();
            log.info("HubEventProcessor завершил работу");
        }
    }

    @PreDestroy
    @Override
    public void destroy() {
        log.info("Получен сигнал завершения работы HubEventProcessor");
        this.running = false;
        if (processorFuture != null && !processorFuture.isDone()) {
            processorFuture.cancel(true);
        }
        consumer.wakeup();
    }
}