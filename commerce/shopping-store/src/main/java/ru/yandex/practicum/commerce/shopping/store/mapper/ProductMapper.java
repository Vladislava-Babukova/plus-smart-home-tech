package ru.yandex.practicum.commerce.shopping.store.mapper;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductDto;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductState;
import ru.yandex.practicum.commerce.shopping.store.model.ProductEntity;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ProductMapper {


    public static ProductDto toDto(ProductEntity entity) {
        if (entity == null) {
            return null;
        }

        return ProductDto.builder()
                .productId(entity.getProductId())
                .productName(entity.getProductName())
                .description(entity.getDescription())
                .imageSrc(entity.getImageSrc())
                .quantityState(entity.getQuantityState())
                .productState(entity.getProductState())
                .productCategory(entity.getProductCategory())
                .price(entity.getPrice())
                .build();
    }


    public static ProductEntity toEntity(ProductDto dto) {
        if (dto == null) {
            return null;
        }

        return ProductEntity.builder()
                .productId(dto.getProductId())
                .productName(dto.getProductName())
                .description(dto.getDescription())
                .imageSrc(dto.getImageSrc())
                .quantityState(dto.getQuantityState())
                .productState(dto.getProductState() != null ? dto.getProductState() : ProductState.ACTIVE)
                .productCategory(dto.getProductCategory())
                .price(dto.getPrice())
                .build();
    }


    public static void updateEntityFromDto(ProductEntity entity, ProductDto dto) {
        if (entity == null || dto == null) {
            return;
        }

        entity.setProductName(dto.getProductName());
        entity.setDescription(dto.getDescription());
        entity.setImageSrc(dto.getImageSrc());
        entity.setQuantityState(dto.getQuantityState());
        entity.setProductCategory(dto.getProductCategory());
        entity.setPrice(dto.getPrice());
    }


    public static ProductEntity toNewEntity(String productName, String description,
                                            String productCategory, String price) {
        return ProductEntity.builder()
                .productName(productName)
                .description(description)
                .productState(ProductState.ACTIVE)
                .build();
    }
}
