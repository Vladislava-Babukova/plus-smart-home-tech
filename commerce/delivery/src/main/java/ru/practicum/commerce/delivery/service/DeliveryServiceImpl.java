package ru.practicum.commerce.delivery.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.practicum.commerce.delivery.dal.DeliveryRepository;
import ru.practicum.commerce.delivery.exception.NoDeliveryFoundBusinessException;
import ru.practicum.commerce.delivery.mapper.DeliveryMapper;
import ru.practicum.commerce.delivery.model.DeliveryEntity;
import ru.yandex.practicum.commerce.contract.order.OrderClient;
import ru.yandex.practicum.commerce.contract.warehouse.WarehouseClient;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryDto;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryState;
import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.dto.warehouse.AddressDto;
import ru.yandex.practicum.commerce.dto.warehouse.ShippedToDeliveryRequest;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryServiceImpl implements DeliveryService {

    private final DeliveryRepository deliveryRepository;
    private final WarehouseClient warehouseClient;
    private final OrderClient orderClient;

    private static final BigDecimal BASE_COST = new BigDecimal("5.0");
    private static final BigDecimal FRAGILE_MULTIPLIER = new BigDecimal("0.2");
    private static final BigDecimal WEIGHT_MULTIPLIER = new BigDecimal("0.3");
    private static final BigDecimal VOLUME_MULTIPLIER = new BigDecimal("0.2");
    private static final BigDecimal ADDRESS_MULTIPLIER = new BigDecimal("0.2");
    private static final int SCALE = 2;
    private static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;

    @Override
    @Transactional
    public DeliveryDto createDelivery(DeliveryDto deliveryDto) {
        log.info("Creating delivery for order: {}", deliveryDto.getOrderId());

        deliveryRepository.findByOrderId(deliveryDto.getOrderId())
                .ifPresent(existingDelivery -> {
                    throw new RuntimeException("Delivery already exists for order: " + deliveryDto.getOrderId());
                });

        DeliveryEntity deliveryEntity = DeliveryMapper.toEntity(deliveryDto);
        DeliveryEntity savedDelivery = deliveryRepository.save(deliveryEntity);

        DeliveryDto result = DeliveryMapper.toDto(savedDelivery);
        log.info("Created delivery with id: {} for order: {}", result.getDeliveryId(), deliveryDto.getOrderId());

        return result;
    }

    @Override
    @Transactional
    public BigDecimal calculateDeliveryCost(OrderDto orderDto) {
        log.info("Calculating delivery cost for order: {}", orderDto.getOrderId());

        AddressDto warehouseAddressDto;
        try {
            warehouseAddressDto = warehouseClient.getWarehouseAddress();
            log.debug("Warehouse address received: {}", warehouseAddressDto.getStreet());
        } catch (Exception e) {
            log.error("Failed to get warehouse address: {}", e.getMessage());
            throw new RuntimeException("Failed to get warehouse address: " + e.getMessage(), e);
        }

        String deliveryStreet = getDeliveryStreetFromDatabase(orderDto.getOrderId());

        BigDecimal cost = calculateDeliveryCostAlgorithm(
                warehouseAddressDto.getStreet(),
                orderDto.getDeliveryWeight() != null ? orderDto.getDeliveryWeight() : 0.0,
                orderDto.getDeliveryVolume() != null ? orderDto.getDeliveryVolume() : 0.0,
                orderDto.getFragile() != null ? orderDto.getFragile() : false,
                deliveryStreet
        );

        DeliveryEntity savedCost = updateDeliveryCostByOrderId(orderDto.getOrderId(), cost);

        log.debug("Calculated delivery cost for order {}: {}", orderDto.getOrderId(), cost);
        return cost.setScale(SCALE, ROUNDING_MODE);
    }

    @Override
    @Transactional
    public void processDeliveryPicked(UUID orderId) {
        log.info("Processing delivery picked for order: {}", orderId);

        DeliveryEntity delivery = getDeliveryByOrderIdEntity(orderId);

        DeliveryEntity updatedDelivery = deliveryRepository.save(delivery);

        ShippedToDeliveryRequest shippedRequest = ShippedToDeliveryRequest.builder()
                .orderId(orderId)
                .deliveryId(delivery.getDeliveryId())
                .build();
        try {
            warehouseClient.shippedToDelivery(shippedRequest);
            delivery.setDeliveryState(DeliveryState.IN_PROGRESS);
            log.debug("Warehouse notified about delivery shipment for order: {}", orderId);

        } catch (Exception e) {
            log.error("Failed to process delivery picked for order: {}. Error: {}",
                    orderId, e.getMessage());
            delivery.setDeliveryState(DeliveryState.FAILED);
            throw new RuntimeException("Failed to process delivery picked: " + e.getMessage(), e);
        }
        log.info("Delivery for order {} marked as IN_PROGRESS", orderId);
    }

    @Override
    @Transactional
    public void processDeliverySuccess(UUID orderId) {
        log.info("Processing delivery success for order: {}", orderId);

        DeliveryEntity delivery = getDeliveryByOrderIdEntity(orderId);
        delivery.setDeliveryState(DeliveryState.DELIVERED);

        DeliveryEntity updatedDelivery = deliveryRepository.save(delivery);

        try {
            OrderDto updatedOrder = orderClient.delivery(orderId);
            if (updatedOrder != null) {
                log.debug("Successfully updated order status for order: {}", orderId);
            } else {
                log.error("Failed to update order status - returned null for order: {}", orderId);
                throw new RuntimeException("Order service returned null response for order: " + orderId);
            }
        } catch (Exception e) {
            log.error("Failed to update order status in order service for order: {}. Error: {}",
                    orderId, e.getMessage());
            throw new RuntimeException("Failed to update order status: " + e.getMessage(), e);
        }
        log.info("Delivery for order {} marked as DELIVERED", orderId);
    }

    @Override
    @Transactional
    public void processDeliveryFailed(UUID orderId) {
        log.info("Processing delivery failure for order: {}", orderId);

        DeliveryEntity delivery = getDeliveryByOrderIdEntity(orderId);
        delivery.setDeliveryState(DeliveryState.FAILED);

        DeliveryEntity updatedDelivery = deliveryRepository.save(delivery);

        try {
            OrderDto updatedOrder = orderClient.deliveryFailed(orderId);
            if (updatedOrder != null) {
                log.debug("Successfully updated order status to failed for order: {}", orderId);
            } else {
                log.error("Failed to update order status - returned null for order: {}", orderId);
                throw new RuntimeException("Order service returned null response for order: " + orderId);
            }
        } catch (Exception e) {
            log.error("Failed to update order status in order service for order: {}. Error: {}",
                    orderId, e.getMessage());
            throw new RuntimeException("Failed to update order status: " + e.getMessage(), e);
        }
        log.info("Delivery for order {} marked as FAILED", orderId);
    }

    @Override
    @Transactional(readOnly = true)
    public DeliveryDto getDeliveryById(UUID deliveryId) {
        DeliveryEntity delivery = getDeliveryEntity(deliveryId);
        return DeliveryMapper.toDto(delivery);
    }

    @Override
    @Transactional(readOnly = true)
    public DeliveryDto getDeliveryByOrderId(UUID orderId) {
        DeliveryEntity delivery = getDeliveryByOrderIdEntity(orderId);
        return DeliveryMapper.toDto(delivery);
    }

    private DeliveryEntity getDeliveryEntity(UUID deliveryId) {
        return deliveryRepository.findById(deliveryId)
                .orElseThrow(() -> new NoDeliveryFoundBusinessException(deliveryId));
    }

    private DeliveryEntity getDeliveryByOrderIdEntity(UUID orderId) {
        return deliveryRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoDeliveryFoundBusinessException(null,
                        "Delivery not found for order: " + orderId));
    }

    private String getDeliveryStreetFromDatabase(UUID orderId) {
        try {
            DeliveryEntity delivery = deliveryRepository.findByOrderId(orderId)
                    .orElseThrow(() -> new NoDeliveryFoundBusinessException(null,
                            "Delivery not found for order: " + orderId));

            if (delivery.getToAddress() != null && delivery.getToAddress().getStreet() != null) {
                return delivery.getToAddress().getStreet();
            } else {
                log.warn("Delivery address not found for order: {}, using default street", orderId);
                return "";
            }
        } catch (NoDeliveryFoundBusinessException e) {
            log.warn("No delivery found for order: {}, cannot get delivery address", orderId);
            throw new RuntimeException("Delivery not found for order: " + orderId + ". Please create delivery first.");
        }
    }

    private BigDecimal calculateDeliveryCostAlgorithm(String warehouseAddress,
                                                      Double weight,
                                                      Double volume,
                                                      Boolean fragile,
                                                      String deliveryStreet) {
        BigDecimal cost = BASE_COST;


        BigDecimal addressMultiplier;
        if (warehouseAddress.contains("ADDRESS_1")) {
            addressMultiplier = BigDecimal.ONE;
        } else if (warehouseAddress.contains("ADDRESS_2")) {
            addressMultiplier = new BigDecimal("2");
        } else {
            addressMultiplier = BigDecimal.ONE;
        }

        cost = cost.multiply(addressMultiplier).add(BASE_COST);

        if (fragile != null && fragile) {
            BigDecimal fragileCost = cost.multiply(FRAGILE_MULTIPLIER);
            cost = cost.add(fragileCost);
        }

        if (weight != null) {
            BigDecimal weightCost = BigDecimal.valueOf(weight).multiply(WEIGHT_MULTIPLIER);
            cost = cost.add(weightCost);
        }

        if (volume != null) {
            BigDecimal volumeCost = BigDecimal.valueOf(volume).multiply(VOLUME_MULTIPLIER);
            cost = cost.add(volumeCost);
        }

        if (deliveryStreet != null && !deliveryStreet.isEmpty() && !deliveryStreet.equals(warehouseAddress)) {
            BigDecimal addressCost = cost.multiply(ADDRESS_MULTIPLIER);
            cost = cost.add(addressCost);
        }

        return cost;
    }


    private DeliveryEntity updateDeliveryCostByOrderId(UUID orderId, BigDecimal deliveryCost) {
        log.info("Updating delivery cost for order: {} to {}", orderId, deliveryCost);

        DeliveryEntity delivery = getDeliveryByOrderIdEntity(orderId);
        delivery.setDeliveryCost(deliveryCost);

        DeliveryEntity updatedDelivery = deliveryRepository.save(delivery);
        log.info("Delivery cost updated for order: {}", orderId);

        return updatedDelivery;
    }
}
