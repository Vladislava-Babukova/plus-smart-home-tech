package ru.practicum.commerce.delivery.service;

import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;
import ru.yandex.practicum.commerce.dto.order.OrderDto;

import java.math.BigDecimal;
import java.util.UUID;

public interface DeliveryService {

    DeliveryDto createDelivery(DeliveryDto deliveryDto);

    BigDecimal calculateDeliveryCost(OrderDto orderDto);

    void processDeliveryPicked(UUID orderId);

    void processDeliverySuccess(UUID orderId);

    void processDeliveryFailed(UUID orderId);

    DeliveryDto getDeliveryById(UUID deliveryId);

    DeliveryDto getDeliveryByOrderId(UUID orderId);
}
