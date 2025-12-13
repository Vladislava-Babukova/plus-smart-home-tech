package ru.yandex.practicum.commerce.dto.warehouse;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NewProductInWarehouseRequest {
    private UUID productId;
    private Boolean fragile;
    private DimensionDto dimension;
    private Double weight;
}