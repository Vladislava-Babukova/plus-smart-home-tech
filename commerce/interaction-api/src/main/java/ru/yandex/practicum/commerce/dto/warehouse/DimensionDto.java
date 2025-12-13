package ru.yandex.practicum.commerce.dto.warehouse;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DimensionDto {
    private Double width;
    private Double height;
    private Double depth;
}
