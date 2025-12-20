package ru.yandex.practicum.commerce.order.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.contract.payment.PaymentClient;
import ru.yandex.practicum.commerce.contract.warehouse.WarehouseClient;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryState;
import ru.yandex.practicum.commerce.dto.order.CreateNewOrderRequest;
import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.dto.order.OrderState;
import ru.yandex.practicum.commerce.dto.order.ProductReturnRequest;
import ru.yandex.practicum.commerce.dto.payment.PaymentDto;
import ru.yandex.practicum.commerce.dto.shopping.cart.ShoppingCartDto;
import ru.yandex.practicum.commerce.dto.warehouse.AddressDto;
import ru.yandex.practicum.commerce.dto.warehouse.AssemblyProductsForOrderRequest;
import ru.yandex.practicum.commerce.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.commerce.order.dal.OrderItemRepository;
import ru.yandex.practicum.commerce.order.dal.OrderRepository;
import ru.yandex.practicum.commerce.order.exception.NoOrderFoundBusinessException;
import ru.yandex.practicum.commerce.order.exception.NotAuthorizedBusinessException;
import ru.yandex.practicum.commerce.order.mapper.OrderMapper;
import ru.yandex.practicum.commerce.order.model.OrderEntity;
import ru.yandex.practicum.commerce.order.model.OrderItemEntity;
import ru.yandex.practicum.commerce.contract.delivery.DeliveryClient;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final WarehouseClient warehouseClient;
    private final PaymentClient paymentClient;
    private final DeliveryClient deliveryClient;

    @Override
    @Transactional(readOnly = true)
    public @org.jetbrains.annotations.NotNull List<Object> getClientOrders(String username) {
        validateUsername(username);
        log.info("Getting orders for user: {}", username);
        List<OrderEntity> orders = orderRepository.findByUsernameOrderByCreatedAtDesc(username);

        return orders.stream()
                .map(order -> {
                    List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(order.getOrderId());
                    return OrderMapper.toDto(order, items);
                })
                .collect(java.util.stream.Collectors.toList());
    }

    @Override
    @Transactional
    public OrderDto createNewOrder(String username, CreateNewOrderRequest request) {
        log.info("Creating new order from shopping cart: {}", request.getShoppingCart().getShoppingCartId());
        validateUsername(username);

        ShoppingCartDto tempCart = ShoppingCartDto.builder()
                .shoppingCartId(request.getShoppingCart().getShoppingCartId())
                .products(new HashMap<>(request.getShoppingCart().getProducts()))
                .build();
        BookedProductsDto bookedProductsDto;

        try {
            bookedProductsDto =  warehouseClient.checkProductQuantityEnoughForShoppingCart(tempCart);
            log.debug("Products availability confirmed by warehouse");
        } catch (Exception e) {
            log.error("Failed to check product availability in warehouse: {}", e.getMessage());
            throw new RuntimeException("Product availability check failed: " + e.getMessage(), e);
        }

        OrderEntity order = OrderEntity.builder()
                .username(username)
                .shoppingCartId(request.getShoppingCart().getShoppingCartId())
                .orderState(OrderState.NEW)
                .deliveryWeight(bookedProductsDto.getDeliveryWeight())
                .deliveryVolume(bookedProductsDto.getDeliveryVolume())
                .fragile(bookedProductsDto.getFragile())
                .country(request.getDeliveryAddress().getCountry())
                .city(request.getDeliveryAddress().getCity())
                .street(request.getDeliveryAddress().getStreet())
                .house(request.getDeliveryAddress().getHouse())
                .flat(request.getDeliveryAddress().getFlat())
                .build();

        OrderEntity savedOrder = orderRepository.save(order);

        List<OrderItemEntity> orderItems = OrderMapper.toOrderItemEntities(
                savedOrder,
                request.getShoppingCart().getProducts()
        );
        orderItemRepository.saveAll(orderItems);

        AssemblyProductsForOrderRequest assemblyRequest = AssemblyProductsForOrderRequest.builder()
                .orderId(savedOrder.getOrderId())
                .products(request.getShoppingCart().getProducts())
                .build();
        try {
            BookedProductsDto assemblyResult = warehouseClient.assemblyProductsForOrder(assemblyRequest);
            log.debug("Products assembled for order: {}", savedOrder.getOrderId());

            OrderDto assembled = assembly(savedOrder.getOrderId());

            savedOrder.setDeliveryWeight(assemblyResult.getDeliveryWeight());
            savedOrder.setDeliveryVolume(assemblyResult.getDeliveryVolume());
            savedOrder.setFragile(assemblyResult.getFragile());

        } catch (Exception e) {
            log.error("Failed to assemble products for order {}: {}", savedOrder.getOrderId(), e.getMessage());

            OrderDto assembled = assemblyFailed(savedOrder.getOrderId());

            throw new RuntimeException("Product assembly failed: " + e.getMessage(), e);
        }

        DeliveryDto createdDelivery;
        try {
            AddressDto warehouseAddress = warehouseClient.getWarehouseAddress();
            log.debug("Warehouse address received: {}", warehouseAddress);

            DeliveryDto deliveryRequest = DeliveryDto.builder()
                    .orderId(savedOrder.getOrderId())
                    .fromAddress(warehouseAddress) // адрес склада
                    .toAddress(request.getDeliveryAddress()) // адрес клиента из запроса
                    .deliveryState(DeliveryState.CREATED)
                    .build();

            createdDelivery = deliveryClient.delivery(deliveryRequest);
            savedOrder.setDeliveryId(createdDelivery.getDeliveryId());
            log.debug("Delivery created with ID: {}", createdDelivery.getDeliveryId());
        } catch (Exception e) {
            log.error("Failed to create delivery: {}", e.getMessage());
            throw new RuntimeException("Delivery creation failed: " + e.getMessage(), e);
        }

        OrderDto orderDtoForDelivery = OrderMapper.toDto(savedOrder, orderItems);
        BigDecimal deliveryCost;
        try {
            deliveryCost = deliveryClient.deliveryCost(orderDtoForDelivery);
            savedOrder.setDeliveryPrice(deliveryCost);
            log.debug("Delivery cost calculated: {}", deliveryCost);
        } catch (Exception e) {
            log.error("Failed to calculate delivery cost: {}", e.getMessage());
            throw new RuntimeException("Delivery cost calculation failed: " + e.getMessage(), e);
        }

        OrderEntity updatedOrder = orderRepository.save(savedOrder);

        OrderDto orderDtoForPayment = OrderMapper.toDto(updatedOrder, orderItems);
        PaymentDto paymentDto;
        try {
            paymentDto = paymentClient.payment(orderDtoForPayment);
            updatedOrder.setPaymentId(paymentDto.getPaymentId());
            updatedOrder.setOrderState(OrderState.ON_PAYMENT); // Меняем статус на "ожидает оплаты"
            updatedOrder.setTotalPrice(paymentDto.getTotalPayment());
            updatedOrder.setProductPrice(paymentDto.getTotalPayment().subtract(paymentDto.getDeliveryTotal())
                    .subtract(paymentDto.getFeeTotal()));
            log.debug("Payment process started with payment ID: {}", paymentDto.getPaymentId());
        } catch (Exception e) {
            log.error("Failed to create payment: {}", e.getMessage());
            throw new RuntimeException("Payment creation failed: " + e.getMessage(), e);
        }

        OrderEntity finalOrder = orderRepository.save(updatedOrder);

        OrderDto result = OrderMapper.toDto(finalOrder, orderItems);
        log.info("Created new order with id: {} and payment id: {}",
                result.getOrderId(), result.getPaymentId());

        return result;
    }

    @Override
    @Transactional
    public OrderDto productReturn(ProductReturnRequest request) {
        log.info("Processing product return for order: {}", request.getOrderId());

        OrderEntity order = getOrderEntity(request.getOrderId());
        order.setOrderState(OrderState.PRODUCT_RETURNED);

        OrderEntity updatedOrder = orderRepository.save(order);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(updatedOrder.getOrderId());

        try {
            Map<UUID, Integer> returnedProducts = new HashMap<>(request.getProducts());

            warehouseClient.acceptReturn(returnedProducts);
            log.debug("All products successfully returned to warehouse: {}", returnedProducts);
        } catch (Exception e) {
            log.error("Failed to return products to warehouse: {}", e.getMessage());
            throw new RuntimeException("Product return to warehouse failed: " + e.getMessage(), e);
        }
        return OrderMapper.toDto(updatedOrder, items);
    }

    @Override
    @Transactional
    public OrderDto payment(UUID orderId) {
        log.info("Processing payment success for order: {}", orderId);

        OrderEntity order = getOrderEntity(orderId);
        if (order.getOrderState() != OrderState.ON_PAYMENT) {
            log.warn("Order {} is in state {}, but expected ON_PAYMENT",
                    orderId, order.getOrderState());
        }
        order.setOrderState(OrderState.PAID);

        OrderEntity updatedOrder = orderRepository.save(order);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(updatedOrder.getOrderId());
        log.info("Order {} successfully paid", orderId);
        try {
            deliveryClient.deliveryPicked(orderId);
            log.debug("Delivery notified about delivery shipment for order: {}", orderId);

        } catch (Exception e) {
            log.error("Failed to process delivery picked for order in payment service: {}. Error: {}",
                    orderId, e.getMessage());

            throw new RuntimeException("Failed to process delivery picked from payment service: " + e.getMessage(), e);
        }
        log.info("Delivery {} successfully requested", orderId);
        return OrderMapper.toDto(updatedOrder, items);
    }

    @Override
    @Transactional
    public OrderDto paymentFailed(UUID orderId) {
        log.info("Processing payment failure for order: {}", orderId);

        OrderEntity order = getOrderEntity(orderId);
        order.setOrderState(OrderState.PAYMENT_FAILED);

        OrderEntity updatedOrder = orderRepository.save(order);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(updatedOrder.getOrderId());
        log.info("Order {} payment failed", orderId);
        return OrderMapper.toDto(updatedOrder, items);
    }

    @Override
    @Transactional
    public OrderDto delivery(UUID orderId) {
        log.info("Processing delivery for order: {}", orderId);

        OrderEntity order = getOrderEntity(orderId);
        order.setOrderState(OrderState.DELIVERED);

        OrderEntity updatedOrder = orderRepository.save(order);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(updatedOrder.getOrderId());

        return OrderMapper.toDto(updatedOrder, items);
    }

    @Override
    @Transactional
    public OrderDto deliveryFailed(UUID orderId) {
        log.info("Processing delivery failure for order: {}", orderId);

        OrderEntity order = getOrderEntity(orderId);
        order.setOrderState(OrderState.DELIVERY_FAILED);

        OrderEntity updatedOrder = orderRepository.save(order);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(updatedOrder.getOrderId());

        return OrderMapper.toDto(updatedOrder, items);
    }

    @Override
    @Transactional
    public OrderDto complete(UUID orderId) {
        log.info("Completing order: {}", orderId);

        OrderEntity order = getOrderEntity(orderId);
        order.setOrderState(OrderState.COMPLETED);

        OrderEntity updatedOrder = orderRepository.save(order);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(updatedOrder.getOrderId());

        return OrderMapper.toDto(updatedOrder, items);
    }

    @Override
    @Transactional
    public OrderDto calculateTotalCost(UUID orderId) {
        log.info("Calculating total cost for order: {}", orderId);

        OrderEntity order = getOrderEntity(orderId);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(orderId);

        OrderDto orderDto = OrderMapper.toDto(order, items);

        if (order.getDeliveryPrice() == null) {
            BigDecimal deliveryCost = calculateDeliveryCost(orderId).getDeliveryPrice();
            order.setDeliveryPrice(deliveryCost);
            orderDto.setDeliveryPrice(deliveryCost);
            log.debug("Delivery cost calculated during total cost calculation: {}", deliveryCost);
        }


        BigDecimal totalCost;
        try {
            totalCost = paymentClient.getTotalCost(orderDto);
            log.debug("Successfully calculated total cost from payment service: {}", totalCost);
        } catch (Exception e) {
            log.error("Failed to calculate total cost from payment service for order: {}. Error: {}",
                    orderId, e.getMessage());
            throw new RuntimeException("Failed to calculate total cost: " + e.getMessage(), e);
        }

        order.setTotalPrice(totalCost);
        OrderEntity updatedOrder = orderRepository.save(order);

        log.info("Total cost calculated and saved for order {}: {}", orderId, totalCost);
        return OrderMapper.toDto(updatedOrder, items);
    }


    @Override
    @Transactional
    public OrderDto calculateDeliveryCost(UUID orderId) {
        log.info("Calculating delivery cost for order: {}", orderId);

        OrderEntity order = getOrderEntity(orderId);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(orderId);


        OrderDto orderDto = OrderMapper.toDto(order, items);


        if (order.getTotalPrice() == null) {
            BigDecimal totalCost = calculateTotalCost(orderId).getTotalPrice();
            order.setTotalPrice(totalCost);
            orderDto.setTotalPrice(totalCost);
            log.debug("Total cost calculated during delivery cost calculation: {}", totalCost);
        }


        BigDecimal deliveryCost;
        try {
            deliveryCost = deliveryClient.deliveryCost(orderDto);
            log.debug("Successfully calculated delivery cost from delivery service: {}", deliveryCost);
        } catch (Exception e) {
            log.error("Failed to calculate delivery cost from delivery service for order: {}. Error: {}",
                    orderId, e.getMessage());
            throw new RuntimeException("Failed to calculate delivery cost: " + e.getMessage(), e);
        }

        order.setDeliveryPrice(deliveryCost);
        OrderEntity updatedOrder = orderRepository.save(order);

        return OrderMapper.toDto(updatedOrder, items);
    }

    @Override
    @Transactional
    public OrderDto assembly(UUID orderId) {
        log.info("Processing assembly for order: {}", orderId);

        OrderEntity order = getOrderEntity(orderId);
        order.setOrderState(OrderState.ASSEMBLED);

        OrderEntity updatedOrder = orderRepository.save(order);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(updatedOrder.getOrderId());

        return OrderMapper.toDto(updatedOrder, items);
    }

    @Override
    @Transactional
    public OrderDto assemblyFailed(UUID orderId) {
        log.info("Processing assembly failure for order: {}", orderId);

        OrderEntity order = getOrderEntity(orderId);
        order.setOrderState(OrderState.ASSEMBLY_FAILED);

        OrderEntity updatedOrder = orderRepository.save(order);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(updatedOrder.getOrderId());

        return OrderMapper.toDto(updatedOrder, items);
    }

    @Override
    @Transactional(readOnly = true)
    public OrderDto getOrderById(UUID orderId) {
        OrderEntity order = getOrderEntity(orderId);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(orderId);
        return OrderMapper.toDto(order, items);
    }

    @Override
    @Transactional
    public OrderDto updateOrderState(UUID orderId, String username, OrderState newState) {
        validateUsername(username);

        OrderEntity order = orderRepository.findByOrderIdAndUsername(orderId, username)
                .orElseThrow(() -> new NoOrderFoundBusinessException(orderId,
                        "Order not found for user: " + username));

        order.setOrderState(newState);
        OrderEntity updatedOrder = orderRepository.save(order);
        List<OrderItemEntity> items = orderItemRepository.findByOrderOrderId(orderId);

        return OrderMapper.toDto(updatedOrder, items);
    }

    private OrderEntity getOrderEntity(UUID orderId) {
        return orderRepository.findById(orderId)
                .orElseThrow(() -> new NoOrderFoundBusinessException(orderId));
    }

    private void validateUsername(String username) {
        if (username == null || username.isBlank()) {
            throw new NotAuthorizedBusinessException("Username cannot be empty");
        }
    }
}