package ru.yandex.practicum.commerce.payment.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.contract.order.OrderClient;
import ru.yandex.practicum.commerce.contract.shopping.store.ShoppingStoreClient;
import ru.yandex.practicum.commerce.dto.order.OrderDto;
import ru.yandex.practicum.commerce.dto.payment.PaymentDto;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductDto;
import ru.yandex.practicum.commerce.payment.dal.PaymentRepository;
import ru.yandex.practicum.commerce.payment.exception.NoOrderFoundBusinessException;
import ru.yandex.practicum.commerce.payment.exception.NotEnoughInfoInOrderToCalculateBusinessException;
import ru.yandex.practicum.commerce.payment.mapper.PaymentMapper;
import ru.yandex.practicum.commerce.payment.model.PaymentEntity;
import ru.yandex.practicum.commerce.payment.model.PaymentState;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentServiceImpl implements PaymentService {
    private final PaymentRepository paymentRepository;
    private final ShoppingStoreClient shoppingStoreClient;
    private final OrderClient orderClient;

    private static final BigDecimal VAT_RATE = new BigDecimal("0.10"); // 10% НДС
    private static final int SCALE = 2;
    private static final RoundingMode ROUNDING_MODE = RoundingMode.HALF_UP;

    @Override
    @Transactional
    public PaymentDto createPayment(OrderDto orderDto) {
        log.info("Creating payment for order: {}", orderDto.getOrderId());
        validateOrderForPayment(orderDto);

        paymentRepository.findByOrderId(orderDto.getOrderId())
                .ifPresent(existingPayment -> {
                    throw new NotEnoughInfoInOrderToCalculateBusinessException(
                            "Payment already exists for order: " + orderDto.getOrderId());
                });

        BigDecimal productCost = calculateProductCost(orderDto);
        BigDecimal deliveryCost = orderDto.getDeliveryPrice();
        BigDecimal taxCost = productCost.multiply(VAT_RATE).setScale(SCALE, ROUNDING_MODE);
        BigDecimal totalCost = calculateTotalCost(orderDto);

        PaymentEntity payment = PaymentMapper.createNewPayment(
                orderDto.getOrderId(), productCost, deliveryCost, taxCost, totalCost);

        PaymentEntity savedPayment = paymentRepository.save(payment);

        PaymentDto result = PaymentMapper.toDto(savedPayment);
        log.info("Created payment with id: {} for order: {}", result.getPaymentId(), orderDto.getOrderId());

        return result;
    }

    @Override
    @Transactional(readOnly = true)
    public BigDecimal calculateProductCost(OrderDto orderDto) {
        log.info("Calculating product cost for order: {}", orderDto.getOrderId());
        validateOrderForCalculation(orderDto);

        BigDecimal totalProductCost = orderDto.getProducts().entrySet().parallelStream()
                .map(entry -> {
                    UUID productId = entry.getKey();
                    Integer quantity = entry.getValue();

                    try {
                        ProductDto product = shoppingStoreClient.getProduct(productId);

                        if (product != null && product.getPrice() != null) {
                            return product.getPrice().multiply(BigDecimal.valueOf(quantity));
                        } else {
                            log.warn("Product {} not found or has no price for order: {}",
                                    productId, orderDto.getOrderId());
                            return BigDecimal.ZERO;
                        }
                    } catch (Exception e) {
                        log.warn("Error fetching product {}: {} for order: {}",
                                productId, e.getMessage(), orderDto.getOrderId());
                        return BigDecimal.ZERO;
                    }
                })
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        log.debug("Calculated product cost for order {}: {}", orderDto.getOrderId(), totalProductCost);
        return totalProductCost.setScale(SCALE, ROUNDING_MODE);
    }

    @Override
    @Transactional(readOnly = true)
    public BigDecimal calculateTotalCost(OrderDto orderDto) {
        log.info("Calculating total cost for order: {}", orderDto.getOrderId());
        validateOrderForCalculation(orderDto);

        BigDecimal productCost = calculateProductCost(orderDto);

        BigDecimal deliveryCost = orderDto.getDeliveryPrice();
        if (deliveryCost == null) {
            throw new NotEnoughInfoInOrderToCalculateBusinessException(
                    "Delivery cost is not specified in order: " + orderDto.getOrderId());
        }

        BigDecimal taxCost = productCost.multiply(VAT_RATE).setScale(SCALE, ROUNDING_MODE);

        BigDecimal totalCost = productCost.add(deliveryCost).add(taxCost);

        log.debug("Calculated total cost for order {}: productCost={}, deliveryCost={}, taxCost={}, totalCost={}",
                orderDto.getOrderId(), productCost, deliveryCost, taxCost, totalCost);

        return totalCost.setScale(SCALE, ROUNDING_MODE);
    }

    @Override
    @Transactional
    public void processPaymentSuccess(UUID paymentId) {
        log.info("Processing successful payment: {}", paymentId);

        PaymentEntity payment = getPaymentEntity(paymentId);
        payment.setPaymentState(PaymentState.SUCCESS);

        PaymentEntity updatedPayment = paymentRepository.save(payment);

        try {
            OrderDto updatedOrder = orderClient.payment(payment.getOrderId());
            if (updatedOrder != null) {
                log.debug("Successfully updated order status for order: {}", payment.getOrderId());
            } else {
                log.error("Failed to update order status - returned null for order: {}", payment.getOrderId());
                throw new RuntimeException("Order service returned null response for order: " + payment.getOrderId());
            }
        } catch (Exception e) {
            log.error("Failed to update order status in order service for order: {}. Error: {}",
                    payment.getOrderId(), e.getMessage());
            throw new RuntimeException("Failed to update order status: " + e.getMessage(), e);
        }
        log.info("Payment {} marked as SUCCESS for order: {}", paymentId, payment.getOrderId());
    }

    @Override
    @Transactional
    public void processPaymentFailed(UUID paymentId) {
        log.info("Processing failed payment: {}", paymentId);

        PaymentEntity payment = getPaymentEntity(paymentId);
        payment.setPaymentState(PaymentState.FAILED);

        PaymentEntity updatedPayment = paymentRepository.save(payment);

        try {
            OrderDto updatedOrder = orderClient.paymentFailed(payment.getOrderId());
            if (updatedOrder != null) {
                log.debug("Successfully updated order status to failed for order: {}", payment.getOrderId());
            } else {
                log.error("Failed to update order status - returned null for order: {}", payment.getOrderId());
                throw new RuntimeException("Order service returned null response for order: " + payment.getOrderId());
            }
        } catch (Exception e) {
            log.error("Failed to update order status in order service for order: {}. Error: {}",
                    payment.getOrderId(), e.getMessage());
            throw new RuntimeException("Failed to update order status: " + e.getMessage(), e);
        }
        log.info("Payment {} marked as FAILED for order: {}", paymentId, payment.getOrderId());
    }

    @Override
    @Transactional(readOnly = true)
    public PaymentDto getPaymentById(UUID paymentId) {
        PaymentEntity payment = getPaymentEntity(paymentId);
        return PaymentMapper.toDto(payment);
    }

    @Override
    @Transactional(readOnly = true)
    public PaymentDto getPaymentByOrderId(UUID orderId) {
        PaymentEntity payment = paymentRepository.findByOrderId(orderId)
                .orElseThrow(() -> new NoOrderFoundBusinessException(null,
                        "Payment not found for order: " + orderId));
        return PaymentMapper.toDto(payment);
    }

    private PaymentEntity getPaymentEntity(UUID paymentId) {
        return paymentRepository.findById(paymentId)
                .orElseThrow(() -> new NoOrderFoundBusinessException(paymentId));
    }

    private void validateOrderForCalculation(OrderDto orderDto) {
        if (orderDto == null) {
            throw new NotEnoughInfoInOrderToCalculateBusinessException("Order cannot be null");
        }

        if (orderDto.getOrderId() == null) {
            throw new NotEnoughInfoInOrderToCalculateBusinessException("Order ID cannot be null");
        }

        if (orderDto.getProducts() == null || orderDto.getProducts().isEmpty()) {
            throw new NotEnoughInfoInOrderToCalculateBusinessException(
                    "Order must contain products: " + orderDto.getOrderId());
        }
    }

    private void validateOrderForPayment(OrderDto orderDto) {
        validateOrderForCalculation(orderDto);

        if (orderDto.getDeliveryPrice() == null) {
            throw new NotEnoughInfoInOrderToCalculateBusinessException(
                    "Delivery price is required for payment: " + orderDto.getOrderId());
        }
    }
}
