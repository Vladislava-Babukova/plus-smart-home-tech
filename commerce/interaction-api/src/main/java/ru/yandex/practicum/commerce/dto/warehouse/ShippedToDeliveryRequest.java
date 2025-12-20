package ru.yandex.practicum.commerce.dto.warehouse;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShippedToDeliveryRequest {

    @NotNull
    private UUID orderId;

    @NotNull
    private UUID deliveryId;
}
