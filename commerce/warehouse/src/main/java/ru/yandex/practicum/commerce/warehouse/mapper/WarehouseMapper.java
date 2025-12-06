package ru.yandex.practicum.commerce.warehouse.mapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.commerce.dto.warehouse.DimensionDto;
import ru.yandex.practicum.commerce.dto.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.commerce.warehouse.model.WarehouseProductEntity;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class WarehouseMapper {


    public static WarehouseProductEntity toEntity(NewProductInWarehouseRequest request) {
        if (request == null) {
            return null;
        }

        return WarehouseProductEntity.builder()
                .productId(request.getProductId())
                .fragile(request.getFragile())
                .width(request.getDimension() != null ? request.getDimension().getWidth() : null)
                .height(request.getDimension() != null ? request.getDimension().getHeight() : null)
                .depth(request.getDimension() != null ? request.getDimension().getDepth() : null)
                .weight(request.getWeight())
                .quantity(0L)
                .build();
    }


    public static DimensionDto toDimensionDto(WarehouseProductEntity entity) {
        if (entity == null) {
            return null;
        }

        return DimensionDto.builder()
                .width(entity.getWidth())
                .height(entity.getHeight())
                .depth(entity.getDepth())
                .build();
    }
}
