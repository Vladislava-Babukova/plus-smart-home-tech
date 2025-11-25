package ru.yandex.practicum.telemetry.analyzer.config;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

@Configuration
@EnableConfigurationProperties(KafkaConfig.class)
public class KafkaBeansConfig {

    @Bean
    public KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer(KafkaConfig kafkaConfig) {
        KafkaConfig.ConsumerConfig config = kafkaConfig.getConsumers().get("SnapshotProcessor");
        return new KafkaConsumer<>(config.getProperties());
    }

    @Bean
    public KafkaConsumer<String, HubEventAvro> hubEventConsumer(KafkaConfig kafkaConfig) {
        KafkaConfig.ConsumerConfig config = kafkaConfig.getConsumers().get("HubEventProcessor");
        return new KafkaConsumer<>(config.getProperties());
    }

    @Bean
    public KafkaConsumer<String, SensorEventAvro> sensorEventConsumer(KafkaConfig kafkaConfig) {
        // для aggregator, если нужно
        KafkaConfig.ConsumerConfig config = kafkaConfig.getConsumers().get("SensorEventProcessor");
        return new KafkaConsumer<>(config.getProperties());
    }
}
