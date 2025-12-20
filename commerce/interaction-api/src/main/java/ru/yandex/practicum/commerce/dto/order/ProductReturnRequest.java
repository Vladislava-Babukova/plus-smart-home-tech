package ru.yandex.practicum.commerce.dto.order;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ProductReturnRequest {
    private UUID orderId;
    private Map<UUID, Integer> products;
}