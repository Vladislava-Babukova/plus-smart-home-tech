package ru.yandex.practicum.telemetry.aggregator.config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.Properties;


@ConfigurationProperties(prefix = "aggregator.kafka")
@Getter
@AllArgsConstructor
public class KafkaAggregatorConfig {
    private final ProducerConfig producer;
    private final ConsumerConfig consumer;

    @Getter
    @Setter
    @AllArgsConstructor
    public static class ProducerConfig {
        private Properties properties;
        private String topic;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class ConsumerConfig {
        private Properties properties;
        private String topic;
        private Duration pollTimeout;
    }
}
