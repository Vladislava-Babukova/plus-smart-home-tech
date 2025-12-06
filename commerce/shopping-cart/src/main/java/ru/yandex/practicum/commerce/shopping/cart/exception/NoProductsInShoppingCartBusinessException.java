package ru.yandex.practicum.commerce.shopping.cart.exception;

import java.util.List;
import java.util.UUID;

public class NoProductsInShoppingCartBusinessException extends RuntimeException {

    private final List<UUID> productIds;

    public NoProductsInShoppingCartBusinessException(List<UUID> productIds) {
        super("Products not found in shopping cart: " + productIds);
        this.productIds = productIds;
    }

    public NoProductsInShoppingCartBusinessException(List<UUID> productIds, String message) {
        super(message);
        this.productIds = productIds;
    }

    public List<UUID> getProductIds() {
        return productIds;
    }
}
