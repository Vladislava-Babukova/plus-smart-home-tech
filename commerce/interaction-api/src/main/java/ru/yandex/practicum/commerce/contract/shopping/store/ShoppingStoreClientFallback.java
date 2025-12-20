package ru.yandex.practicum.commerce.contract.shopping.store;

import org.springframework.data.domain.Page;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductCategory;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductDto;
import ru.yandex.practicum.commerce.dto.shopping.store.QuantityState;

import java.util.UUID;

public class ShoppingStoreClientFallback implements ShoppingStoreClient {
    @Override
    public Page<ProductDto> getProducts(ProductCategory category, int page, int size, String sort) {
        throw new RuntimeException("Shopping-store service is unavailable");
    }

    @Override
    public ProductDto createNewProduct(ProductDto productDto) {
        throw new RuntimeException("Shopping-store service is unavailable");
    }

    @Override
    public ProductDto updateProduct(ProductDto productDto) {
        throw new RuntimeException("Shopping-store service is unavailable");
    }

    @Override
    public boolean removeProductFromStore(UUID productId) {
        throw new RuntimeException("Shopping-store service is unavailable");
    }

    @Override
    public boolean setProductQuantityState(UUID productId, QuantityState quantityState) {
        throw new RuntimeException("Shopping-store service is unavailable");
    }

    @Override
    public ProductDto getProduct(UUID productId) {
        throw new RuntimeException("Shopping-store service is unavailable");
    }
}