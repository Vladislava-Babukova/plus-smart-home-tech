package ru.yandex.practicum.commerce.dto.shopping.store;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Positive;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductDto {

    private UUID productId;

    @NotBlank(message = "Поле наименование товара должно быть заполнено")
    private String productName;

    @NotBlank(message = "Поле описание товара должно быть заполнено")
    private String description;
    private String imageSrc;

    @NotNull(message = "Поле состояние остатка не может быть null")
    private QuantityState quantityState;

    @NotNull(message = "Поле статус товара не может быть null")
    private ProductState productState;
    private ProductCategory productCategory;

    @NotNull(message = "Поле цена товара не может быть null")
    @Positive(message = "Цена должна быть больше 0")
    private BigDecimal price;
}