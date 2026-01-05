package ru.yandex.practicum.commerce.payment.service;

import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.dto.payment.PaymentDto;

import java.math.BigDecimal;
import java.util.UUID;

public interface PaymentService {
    BigDecimal calculateProductCost(OrderDto orderDto);

    BigDecimal calculateTotalCost(OrderDto orderDto);

    PaymentDto createPayment(OrderDto orderDto);

    void processPaymentSuccess(UUID paymentId);

    void processPaymentFailed(UUID paymentId);

    PaymentDto getPaymentById(UUID paymentId);

    PaymentDto getPaymentByOrderId(UUID orderId);
}
