package ru.yandex.practicum.commerce.payment.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.commerce.contract.payment.PaymentOperations;
import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.dto.payment.PaymentDto;
import ru.yandex.practicum.commerce.payment.service.PaymentService;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/payment")
@RequiredArgsConstructor
public class PaymentController implements PaymentOperations {

    private final PaymentService paymentService;

    @Override
    @PostMapping("/productCost")
    public BigDecimal productCost(OrderDto orderDto) {
        log.debug("Calculating product cost for order: {}", orderDto.getOrderId());
        BigDecimal productCost = paymentService.calculateProductCost(orderDto);
        log.debug("Return product cost: {}", productCost);
        return productCost;
    }

    @Override
    @PostMapping("/totalCost")
    public BigDecimal getTotalCost(OrderDto orderDto) {
        log.debug("Calculating total cost for order: {}", orderDto.getOrderId());
        BigDecimal totalCost = paymentService.calculateTotalCost(orderDto);
        log.debug("Return total cost: {}", totalCost);
        return totalCost;
    }

    @Override
    @PostMapping
    public PaymentDto payment(OrderDto orderDto) {
        log.debug("Creating payment for order: {}", orderDto.getOrderId());
        PaymentDto payment = paymentService.createPayment(orderDto);
        log.debug("Return created payment: {}", payment.getPaymentId());
        return payment;
    }

    @Override
    @PostMapping("/refund")
    public void paymentSuccess(UUID paymentId) {
        log.debug("Processing payment success for: {}", paymentId);
        paymentService.processPaymentSuccess(paymentId);
        log.debug("Payment {} successfully processed as SUCCESS", paymentId);
    }

    @Override
    @PostMapping("/failed")
    public void paymentFailed(UUID paymentId) {
        log.debug("Processing payment failure for: {}", paymentId);
        paymentService.processPaymentFailed(paymentId);
        log.debug("Payment {} successfully processed as FAILED", paymentId);
    }
}
