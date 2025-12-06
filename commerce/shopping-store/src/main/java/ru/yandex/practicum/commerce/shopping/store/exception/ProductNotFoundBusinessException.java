package ru.yandex.practicum.commerce.shopping.store.exception;

import java.util.UUID;

public class ProductNotFoundBusinessException extends RuntimeException {

    private final UUID productId;

    public ProductNotFoundBusinessException(UUID productId) {
        super("Product not found with id: " + productId);
        this.productId = productId;
    }

    public ProductNotFoundBusinessException(UUID productId, String message) {
        super(message);
        this.productId = productId;
    }

    public UUID getProductId() {
        return productId;
    }
}
