package ru.yandex.practicum.telemetry.analyzer.config;

import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.telemetry.analyzer.service.HubEventProcessor;
import ru.yandex.practicum.telemetry.analyzer.service.SnapshotProcessor;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Service
public class KafkaConsumerStarter {

    private final KafkaConfig kafkaConfig;
    private final HubEventProcessor hubEventProcessor;
    private final SnapshotProcessor snapshotProcessor;

    public KafkaConsumerStarter(KafkaConfig kafkaConfig,
                                HubEventProcessor hubEventProcessor,
                                SnapshotProcessor snapshotProcessor) {
        this.kafkaConfig = kafkaConfig;
        this.hubEventProcessor = hubEventProcessor;
        this.snapshotProcessor = snapshotProcessor;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void startConsumers() {
        Map<String, KafkaConfig.ConsumerConfig> consumers = kafkaConfig.getConsumers();

        // HubEventProcessor implements Runnable - используем run()
        if (consumers.containsKey("HubEventProcessor")) {
            CompletableFuture.runAsync(() -> {
                Thread.currentThread().setName("HubEventHandlerThread");
                hubEventProcessor.run(); // implements Runnable
            });
        }

        // SnapshotProcessor имеет метод start()
        if (consumers.containsKey("SnapshotProcessor")) {
            CompletableFuture.runAsync(() -> {
                Thread.currentThread().setName("SnapshotProcessorThread");
                snapshotProcessor.start(); // метод start()
            });
        }
    }
}