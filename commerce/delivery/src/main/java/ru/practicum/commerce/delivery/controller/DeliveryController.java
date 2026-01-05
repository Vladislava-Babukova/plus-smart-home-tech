package ru.practicum.commerce.delivery.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.practicum.commerce.delivery.service.DeliveryService;
import ru.yandex.practicum.commerce.contract.delivery.DeliveryOperations;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;
import ru.yandex.practicum.commerce.dto.order.OrderDto;

import java.math.BigDecimal;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/delivery")
@RequiredArgsConstructor
public class DeliveryController implements DeliveryOperations {

    private final DeliveryService deliveryService;

    @Override
    @PutMapping
    public DeliveryDto delivery(@RequestBody DeliveryDto deliveryDto) {
        log.debug("Planning delivery for order: {}", deliveryDto.getOrderId());
        DeliveryDto plannedDelivery = deliveryService.createDelivery(deliveryDto);
        log.debug("Return planned delivery: {}", plannedDelivery.getDeliveryId());
        return plannedDelivery;
    }

    @Override
    @PostMapping("/cost")
    public BigDecimal deliveryCost(@RequestBody OrderDto orderDto) {
        log.debug("Calculating delivery cost for order: {}", orderDto.getOrderId());
        BigDecimal deliveryCost = deliveryService.calculateDeliveryCost(orderDto);
        log.debug("Return delivery cost: {}", deliveryCost);
        return deliveryCost;
    }

    @Override
    @PostMapping("/picked")
    public void deliveryPicked(@RequestBody UUID orderId) {
        log.debug("Processing delivery picked for order: {}", orderId);
        deliveryService.processDeliveryPicked(orderId);
        log.debug("Delivery for order {} successfully processed as PICKED", orderId);
    }

    @Override
    @PostMapping("/successful")
    public void deliverySuccessful(@RequestBody UUID orderId) {
        log.debug("Processing delivery success for order: {}", orderId);
        deliveryService.processDeliverySuccess(orderId);
        log.debug("Delivery for order {} successfully processed as SUCCESSFUL", orderId);
    }

    @Override
    @PostMapping("/failed")
    public void deliveryFailed(@RequestBody UUID orderId) {
        log.debug("Processing delivery failure for order: {}", orderId);
        deliveryService.processDeliveryFailed(orderId);
        log.debug("Delivery for order {} successfully processed as FAILED", orderId);
    }
}
