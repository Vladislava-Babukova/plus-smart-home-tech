package ru.yandex.practicum.commerce.shopping.store;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@ConfigurationPropertiesScan
@EnableFeignClients
public class ShoppingStore {
    public static void main(String[] args) {
        SpringApplication.run(ShoppingStore.class, args);
    }
}
