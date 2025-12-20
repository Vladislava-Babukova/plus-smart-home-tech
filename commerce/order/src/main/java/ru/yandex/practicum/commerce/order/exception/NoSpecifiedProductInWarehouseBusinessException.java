package ru.yandex.practicum.commerce.order.exception;

import java.util.List;
import java.util.UUID;

public class NoSpecifiedProductInWarehouseBusinessException extends RuntimeException {
    private final List<UUID> productIds;

    public NoSpecifiedProductInWarehouseBusinessException(List<UUID> productIds) {
        super("Products not available in warehouse: " + productIds);
        this.productIds = productIds;
    }

    public NoSpecifiedProductInWarehouseBusinessException(List<UUID> productIds, String message) {
        super(message);
        this.productIds = productIds;
    }

    public List<UUID> getProductIds() {
        return productIds;
    }
}
