package ru.yandex.practicum.commerce.order.mapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.order.model.OrderEntity;
import ru.yandex.practicum.commerce.order.model.OrderItemEntity;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class OrderMapper {

    public static OrderDto toDto(OrderEntity entity, List<OrderItemEntity> items) {
        if (entity == null) {
            return null;
        }

        Map<UUID, Integer> products = items.stream()
                .collect(Collectors.toMap(
                        OrderItemEntity::getProductId,
                        OrderItemEntity::getQuantity
                ));

        return OrderDto.builder()
                .orderId(entity.getOrderId())
                .shoppingCartId(entity.getShoppingCartId())
                .products(products)
                .paymentId(entity.getPaymentId())
                .deliveryId(entity.getDeliveryId())
                .state(entity.getOrderState())
                .deliveryWeight(entity.getDeliveryWeight())
                .deliveryVolume(entity.getDeliveryVolume())
                .fragile(entity.getFragile())
                .totalPrice(entity.getTotalPrice())
                .deliveryPrice(entity.getDeliveryPrice())
                .productPrice(entity.getProductPrice())
                .build();
    }

    public static OrderEntity toEntity(OrderDto dto) {
        if (dto == null) {
            return null;
        }

        return OrderEntity.builder()
                .orderId(dto.getOrderId())
                .shoppingCartId(dto.getShoppingCartId())
                .orderState(dto.getState())
                .paymentId(dto.getPaymentId())
                .deliveryId(dto.getDeliveryId())
                .deliveryWeight(dto.getDeliveryWeight())
                .deliveryVolume(dto.getDeliveryVolume())
                .fragile(dto.getFragile())
                .totalPrice(dto.getTotalPrice())
                .deliveryPrice(dto.getDeliveryPrice())
                .productPrice(dto.getProductPrice())
                .build();
    }

    public static List<OrderItemEntity> toOrderItemEntities(OrderEntity order, Map<UUID, Integer> products) {
        if (products == null || products.isEmpty()) {
            return List.of();
        }

        return products.entrySet().stream()
                .map(entry -> OrderItemEntity.builder()
                        .order(order)
                        .productId(entry.getKey())
                        .quantity(entry.getValue())
                        .build())
                .collect(Collectors.toList());
    }

    public static Map<UUID, Integer> toProductMap(List<OrderItemEntity> items) {
        if (items == null || items.isEmpty()) {
            return new HashMap<>();
        }

        return items.stream()
                .collect(Collectors.toMap(
                        OrderItemEntity::getProductId,
                        OrderItemEntity::getQuantity
                ));
    }
}
