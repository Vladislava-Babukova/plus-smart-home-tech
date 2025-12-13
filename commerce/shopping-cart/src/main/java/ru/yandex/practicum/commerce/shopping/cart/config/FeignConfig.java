package ru.yandex.practicum.commerce.shopping.cart.config;

import feign.Feign;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.commerce.contract.warehouse.WarehouseClient;

@Configuration
@EnableFeignClients(clients = WarehouseClient.class)
public class FeignConfig {

    @Bean
    public Feign.Builder feignBuilder() {

        return Feign.builder()
                .errorDecoder(new CustomErrorDecoder());
    }
}
