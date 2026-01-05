package ru.yandex.practicum.commerce.contract.payment;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.dto.payment.PaymentDto;

import java.math.BigDecimal;
import java.util.UUID;

@Component
public class PaymentClientFallback implements PaymentClient {
    @Override
    public BigDecimal productCost(OrderDto orderDto) {
        throw new RuntimeException("Payment service is unavailable");
    }

    @Override
    public BigDecimal getTotalCost(OrderDto orderDto) {
        throw new RuntimeException("Payment service is unavailable");
    }

    @Override
    public PaymentDto payment(OrderDto orderDto) {
        throw new RuntimeException("Payment service is unavailable");
    }

    @Override
    public void paymentSuccess(UUID paymentId) {
        throw new RuntimeException("Payment service is unavailable");
    }

    @Override
    public void paymentFailed(UUID paymentId) {
        throw new RuntimeException("Payment service is unavailable");
    }
}
