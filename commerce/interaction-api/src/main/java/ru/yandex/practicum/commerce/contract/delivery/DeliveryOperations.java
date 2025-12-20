package ru.yandex.practicum.commerce.contract.delivery;

import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;
import ru.yandex.practicum.commerce.dto.order.OrderDto;

import java.math.BigDecimal;
import java.util.UUID;

public interface DeliveryOperations {

    DeliveryDto delivery(DeliveryDto deliveryDto);

    BigDecimal deliveryCost(OrderDto orderDto);

    void deliveryPicked(UUID orderId);

    void deliverySuccessful(UUID orderId);

    void deliveryFailed(UUID orderId);
}
