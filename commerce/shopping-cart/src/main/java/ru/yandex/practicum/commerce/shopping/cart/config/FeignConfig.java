package ru.yandex.practicum.commerce.shopping.cart.config;

import feign.Feign;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class FeignConfig {

    @Bean
    public Feign.Builder feignBuilder() {

        return Feign.builder()
                .errorDecoder(new CustomErrorDecoder());
    }
}
