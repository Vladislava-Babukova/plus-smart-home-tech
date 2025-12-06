package ru.yandex.practicum.telemetry.analyzer.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
@ConfigurationProperties("analyzer.kafka")
@Validated
public class KafkaConfig {
    private final Map<String, ConsumerConfig> consumers;

    public KafkaConfig(
            @Value("#{${analyzer.kafka.common-properties:{}}}") Map<String, String> commonProperties,
            List<ConsumerConfig> consumers) {

        if (consumers == null) {
            throw new IllegalArgumentException("Kafka consumers configuration cannot be null");
        }

        this.consumers = consumers.stream()
                .peek(config -> {
                    Properties mergedProps = new Properties();
                    if (commonProperties != null) {
                        mergedProps.putAll(commonProperties);
                    }
                    if (config.getProperties() != null) {
                        mergedProps.putAll(config.getProperties());
                    }
                    config.setProperties(mergedProps);
                })
                .collect(Collectors.toMap(ConsumerConfig::getType, Function.identity(), (existing, replacement) -> {
                    throw new IllegalArgumentException("Duplicate consumer type found: " + existing.getType());
                }));
    }

    @Setter
    @Getter
    @Valid
    public static class ConsumerConfig {

        @NotBlank
        private String type;

        @NotEmpty
        private List<@NotBlank String> topics;

        @NotNull
        private Duration pollTimeout;

        private Properties properties;

        public ConsumerConfig() {
        }

        public ConsumerConfig(String type, List<String> topics, Duration pollTimeout, Map<String, String> properties) {
            this.type = type;
            this.topics = topics;
            this.pollTimeout = pollTimeout;

            if (properties != null) {
                this.properties = new Properties();
                this.properties.putAll(properties);
            }
        }
    }
}