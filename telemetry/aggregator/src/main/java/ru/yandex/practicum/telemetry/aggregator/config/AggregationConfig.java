package ru.yandex.practicum.telemetry.aggregator.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.telemetry.aggregator.service.AggregationStarter;
import ru.yandex.practicum.telemetry.aggregator.service.SnapshotService;

@Configuration
public class AggregationConfig {
    @Bean
    public SnapshotService snapshotService() {
        return new SnapshotService();
    }

    @Bean
    public AggregationStarter aggregationStarter(SnapshotService snapshotService,
                                                 KafkaAggregatorConfig kafkaConfig) {
        return new AggregationStarter(snapshotService, kafkaConfig);
    }
}
