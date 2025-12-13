package ru.yandex.practicum.telemetry.collector.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "kafka.producer")
@Component
@Data
public class KafkaProducerProperties {
    private String bootstrapServers;
    private String keySerializer;
    private String valueSerializer;
    private String acks;
    private int retries;
    private int maxInFlightRequestsPerConnection;
    private long lingerMs;
    private int batchSize;
}