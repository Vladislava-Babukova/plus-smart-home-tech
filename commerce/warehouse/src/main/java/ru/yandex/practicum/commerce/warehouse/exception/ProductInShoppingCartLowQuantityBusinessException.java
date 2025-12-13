package ru.yandex.practicum.commerce.warehouse.exception;

import java.util.Map;
import java.util.UUID;

public class ProductInShoppingCartLowQuantityBusinessException extends RuntimeException {

    private final Map<UUID, Integer> insufficientProducts;

    public ProductInShoppingCartLowQuantityBusinessException(Map<UUID, Integer> insufficientProducts) {
        super("Insufficient products in warehouse: " + insufficientProducts);
        this.insufficientProducts = insufficientProducts;
    }

    public ProductInShoppingCartLowQuantityBusinessException(Map<UUID, Integer> insufficientProducts, String message) {
        super(message);
        this.insufficientProducts = insufficientProducts;
    }

    public Map<UUID, Integer> getInsufficientProducts() {
        return insufficientProducts;
    }
}
