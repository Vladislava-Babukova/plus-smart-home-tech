package ru.yandex.practicum.commerce.contract.order;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.dto.order.ProductReturnRequest;

import java.util.List;
import java.util.UUID;

@FeignClient(name = "order", path = "/api/v1/order", fallback = OrderClientFallback.class)
public interface OrderClient extends OrderOperations {
    @Override
    @GetMapping
    List<OrderDto> getClientOrders(@RequestParam String username);

    @Override
    @PutMapping
    OrderDto createNewOrder(@RequestParam String username,
                            @RequestBody CreateNewOrderRequest request);

    @Override
    @PostMapping("/return")
    OrderDto productReturn(@RequestBody ProductReturnRequest request);

    @Override
    @PostMapping("/payment")
    OrderDto payment(@RequestBody UUID orderId);

    @Override
    @PostMapping("/payment/failed")
    OrderDto paymentFailed(@RequestBody UUID orderId);

    @Override
    @PostMapping("/delivery")
    OrderDto delivery(@RequestBody UUID orderId);

    @Override
    @PostMapping("/delivery/failed")
    OrderDto deliveryFailed(@RequestBody UUID orderId);

    @Override
    @PostMapping("/completed")
    OrderDto complete(@RequestBody UUID orderId);

    @Override
    @PostMapping("/calculate/total")
    OrderDto calculateTotalCost(@RequestBody UUID orderId);

    @Override
    @PostMapping("/calculate/delivery")
    OrderDto calculateDeliveryCost(@RequestBody UUID orderId);

    @Override
    @PostMapping("/assembly")
    OrderDto assembly(@RequestBody UUID orderId);

    @Override
    @PostMapping("/assembly/failed")
    OrderDto assemblyFailed(@RequestBody UUID orderId);
}