package ru.yandex.practicum.commerce.order.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.dto.order.ProductReturnRequest;
import ru.yandex.practicum.commerce.order.service.OrderService;

import java.util.List;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/order")
@RequiredArgsConstructor
public class OrderController implements OrderOperations {

    private final OrderService orderService;

    @Override
    @GetMapping
    public List<OrderDto> getClientOrders(@RequestParam String username) {
        log.debug("Requested orders for user: {}", username);
        List<OrderDto> orders = orderService.getClientOrders(username);
        log.debug("Return orders count: {}", orders.size());
        return orders;
    }

    @Override
    @PutMapping
    public OrderDto createNewOrder(@RequestParam String username,
                                   @RequestBody CreateNewOrderRequest request) {
        log.debug("Creating new order from shopping cart: {}", request.getShoppingCart().getShoppingCartId());
        OrderDto order = orderService.createNewOrder(username, request);
        log.debug("Return created order: {}", order.getOrderId());
        return order;
    }

    @Override
    @PostMapping("/return")
    public OrderDto productReturn(@RequestBody ProductReturnRequest request) {
        log.debug("Processing product return for order: {}", request.getOrderId());
        OrderDto order = orderService.productReturn(request);
        log.debug("Return updated order after return: {}", order.getOrderId());
        return order;
    }

    @Override
    @PostMapping("/payment")
    public OrderDto payment(@RequestBody UUID orderId) {
        log.debug("Processing payment for order: {}", orderId);
        OrderDto order = orderService.payment(orderId);
        log.debug("Return updated order after payment: {}", order.getOrderId());
        return order;
    }

    @Override
    @PostMapping("/payment/failed")
    public OrderDto paymentFailed(@RequestBody UUID orderId) {
        log.debug("Processing payment failure for order: {}", orderId);
        OrderDto order = orderService.paymentFailed(orderId);
        log.debug("Return updated order after payment failure: {}", order.getOrderId());
        return order;
    }

    @Override
    @PostMapping("/delivery")
    public OrderDto delivery(@RequestBody UUID orderId) {
        log.debug("Processing delivery for order: {}", orderId);
        OrderDto order = orderService.delivery(orderId);
        log.debug("Return updated order after delivery: {}", order.getOrderId());
        return order;
    }

    @Override
    @PostMapping("/delivery/failed")
    public OrderDto deliveryFailed(@RequestBody UUID orderId) {
        log.debug("Processing delivery failure for order: {}", orderId);
        OrderDto order = orderService.deliveryFailed(orderId);
        log.debug("Return updated order after delivery failure: {}", order.getOrderId());
        return order;
    }

    @Override
    @PostMapping("/completed")
    public OrderDto complete(@RequestBody UUID orderId) {
        log.debug("Completing order: {}", orderId);
        OrderDto order = orderService.complete(orderId);
        log.debug("Return completed order: {}", order.getOrderId());
        return order;
    }

    @Override
    @PostMapping("/calculate/total")
    public OrderDto calculateTotalCost(@RequestBody UUID orderId) {
        log.debug("Calculating total cost for order: {}", orderId);
        OrderDto order = orderService.calculateTotalCost(orderId);
        log.debug("Return order with calculated total cost: {}", order.getOrderId());
        return order;
    }

    @Override
    @PostMapping("/calculate/delivery")
    public OrderDto calculateDeliveryCost(@RequestBody UUID orderId) {
        log.debug("Calculating delivery cost for order: {}", orderId);
        OrderDto order = orderService.calculateDeliveryCost(orderId);
        log.debug("Return order with calculated delivery cost: {}", order.getOrderId());
        return order;
    }

    @Override
    @PostMapping("/assembly")
    public OrderDto assembly(@RequestBody UUID orderId) {
        log.debug("Processing assembly for order: {}", orderId);
        OrderDto order = orderService.assembly(orderId);
        log.debug("Return updated order after assembly: {}", order.getOrderId());
        return order;
    }

    @Override
    @PostMapping("/assembly/failed")
    public OrderDto assemblyFailed(@RequestBody UUID orderId) {
        log.debug("Processing assembly failure for order: {}", orderId);
        OrderDto order = orderService.assemblyFailed(orderId);
        log.debug("Return updated order after assembly failure: {}", order.getOrderId());
        return order;
    }
}
