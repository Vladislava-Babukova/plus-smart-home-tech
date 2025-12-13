package ru.yandex.practicum.commerce.warehouse.exception;

import java.util.UUID;

public class NoSpecifiedProductInWarehouseBusinessException extends RuntimeException {

    private final UUID productId;

    public NoSpecifiedProductInWarehouseBusinessException(UUID productId) {
        super("Product not found in warehouse: " + productId);
        this.productId = productId;
    }

    public NoSpecifiedProductInWarehouseBusinessException(UUID productId, String message) {
        super(message);
        this.productId = productId;
    }

    public UUID getProductId() {
        return productId;
    }
}
