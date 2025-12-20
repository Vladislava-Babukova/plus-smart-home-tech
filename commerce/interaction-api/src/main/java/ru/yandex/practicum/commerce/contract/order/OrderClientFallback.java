package ru.yandex.practicum.commerce.contract.order;

import ru.yandex.practicum.commerce.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.dto.order.ProductReturnRequest;

import java.util.List;
import java.util.UUID;

public class OrderClientFallback implements OrderClient{
    @Override
    public List<OrderDto> getClientOrders(String username) {
        throw new RuntimeException("Order service is unavailable");
    }

    @Override
    public OrderDto createNewOrder(String username, CreateNewOrderRequest request) {
        throw new RuntimeException("Order service is unavailable");
    }

    @Override
    public OrderDto productReturn(ProductReturnRequest request) {
        throw new RuntimeException("Order service is unavailable");
    }

    @Override
    public OrderDto payment(UUID orderId) {
        throw new RuntimeException("Order service is unavailable");
    }

    @Override
    public OrderDto paymentFailed(UUID orderId) {
        throw new RuntimeException("Order service is unavailable");
    }

    @Override
    public OrderDto delivery(UUID orderId) {
        throw new RuntimeException("Order service is unavailable");
    }

    @Override
    public OrderDto deliveryFailed(UUID orderId) {
        throw new RuntimeException("Order service is unavailable");
    }

    @Override
    public OrderDto complete(UUID orderId) {
        throw new RuntimeException("Order service is unavailable");
    }

    @Override
    public OrderDto calculateTotalCost(UUID orderId) {
        throw new RuntimeException("Order service is unavailable");
    }

    @Override
    public OrderDto calculateDeliveryCost(UUID orderId) {
        throw new RuntimeException("Order service is unavailable");
    }

    @Override
    public OrderDto assembly(UUID orderId) {
        throw new RuntimeException("Order service is unavailable");
    }

    @Override
    public OrderDto assemblyFailed(UUID orderId) {
        throw new RuntimeException("Order service is unavailable");
    }
}
