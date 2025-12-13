package ru.yandex.practicum.telemetry.collector.service;


import jakarta.annotation.PreDestroy;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.telemetry.collector.config.KafkaProducerProperties;

import java.time.Duration;
import java.util.Properties;

@Component
public class KafkaEventProducer {
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(30);
    private final KafkaProducerProperties kafkaProducerProperties;
    private Producer<String, SpecificRecordBase> producer;

    public KafkaEventProducer(KafkaProducerProperties kafkaProducerProperties) {
        this.kafkaProducerProperties = kafkaProducerProperties;
    }

    public Producer<String, SpecificRecordBase> getProducer() {
        if (producer == null) {
            initProducer();
        }
        return producer;
    }

    private void initProducer() {
        Properties config = new Properties();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProducerProperties.getBootstrapServers());
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, kafkaProducerProperties.getKeySerializer());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, kafkaProducerProperties.getValueSerializer());

        config.put(ProducerConfig.ACKS_CONFIG, kafkaProducerProperties.getAcks());
        config.put(ProducerConfig.RETRIES_CONFIG, kafkaProducerProperties.getRetries());
        config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, kafkaProducerProperties.getMaxInFlightRequestsPerConnection());

        config.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProducerProperties.getLingerMs());
        config.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProducerProperties.getBatchSize());

        producer = new KafkaProducer<>(config);
    }

    @PreDestroy
    public void destroy() {
        stop();
    }

    public void stop() {
        if (producer != null) {
            try {
                producer.flush();
                producer.close(CLOSE_TIMEOUT);
            } finally {
                producer = null;
            }
        }
    }
}
