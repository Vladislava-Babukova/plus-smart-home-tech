package ru.yandex.practicum.commerce.contract.delivery;

import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;
import ru.yandex.practicum.commerce.dto.order.OrderDto;

import java.math.BigDecimal;
import java.util.UUID;

public class DeliveryClientFallback implements DeliveryClient {
    @Override
    public DeliveryDto delivery(DeliveryDto deliveryDto) {
        throw new RuntimeException("Delivery service is unavailable");
    }

    @Override
    public BigDecimal deliveryCost(OrderDto orderDto) {
        throw new RuntimeException("Delivery service is unavailable");
    }

    @Override
    public void deliveryPicked(UUID orderId) {
        throw new RuntimeException("Delivery service is unavailable");
    }

    @Override
    public void deliverySuccessful(UUID orderId) {
        throw new RuntimeException("Delivery service is unavailable");
    }

    @Override
    public void deliveryFailed(UUID orderId) {
        throw new RuntimeException("Delivery service is unavailable");
    }
}
