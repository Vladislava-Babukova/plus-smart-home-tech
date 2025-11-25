package ru.yandex.practicum.telemetry.analyzer.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

@Getter
@ConfigurationProperties("analyzer.kafka")
public class KafkaConfig {
    private final Map<String, ConsumerConfig> consumers;

    public KafkaConfig(Map<String, String> commonProperties, List<ConsumerConfig> consumers) {
        this.consumers = consumers.stream()
                .peek(config -> {
                            Properties mergedProps = new Properties();
                            mergedProps.putAll(commonProperties);
                            mergedProps.putAll(config.getProperties());
                            config.setProperties(mergedProps);
                        }
                )
                .collect(Collectors.toMap(ConsumerConfig::getType, Function.identity()));
    }

    @Setter
    @Getter
    public static class ConsumerConfig {
        private String type;
        private List<String> topics;
        private Duration pollTimeout;
        private Properties properties;

        public ConsumerConfig(String type, List<String> topics, Duration pollTimeout, Map<String, String> properties) {
            this.type = type;
            this.topics = topics;
            this.pollTimeout = pollTimeout;

            this.properties = new Properties(properties.size());
            this.properties.putAll(properties);
        }
    }
}
