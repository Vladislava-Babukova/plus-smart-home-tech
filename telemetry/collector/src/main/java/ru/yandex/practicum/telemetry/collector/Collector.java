package ru.yandex.practicum.telemetry.collector;


import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class Collector {
    public static void main(String[] args) {
        SpringApplication.run(Collector.class, args);
    }
}