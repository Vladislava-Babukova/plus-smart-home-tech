package ru.yandex.practicum.commerce.payment.mapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.commerce.dto.payment.PaymentDto;
import ru.yandex.practicum.commerce.payment.model.PaymentEntity;

import java.math.BigDecimal;
import java.util.UUID;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class PaymentMapper {

    public static PaymentDto toDto(PaymentEntity entity) {
        if (entity == null) {
            return null;
        }

        return PaymentDto.builder()
                .paymentId(entity.getPaymentId())
                .totalPayment(entity.getTotalCost())
                .deliveryTotal(entity.getDeliveryCost())
                .feeTotal(entity.getTaxCost())
                .build();
    }

    public static PaymentEntity toEntity(PaymentDto dto) {
        if (dto == null) {
            return null;
        }

        return PaymentEntity.builder()
                .paymentId(dto.getPaymentId())
                .totalCost(dto.getTotalPayment())
                .deliveryCost(dto.getDeliveryTotal())
                .taxCost(dto.getFeeTotal())
                .build();
    }

    public static PaymentEntity createNewPayment(UUID orderId, BigDecimal productCost,
                                                 BigDecimal deliveryCost, BigDecimal taxCost,
                                                 BigDecimal totalCost) {
        return PaymentEntity.builder()
                .orderId(orderId)
                .productCost(productCost)
                .deliveryCost(deliveryCost)
                .taxCost(taxCost)
                .totalCost(totalCost)
                .build();
    }
}
