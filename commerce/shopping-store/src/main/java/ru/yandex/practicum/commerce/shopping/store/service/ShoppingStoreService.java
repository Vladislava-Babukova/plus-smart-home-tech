package ru.yandex.practicum.commerce.shopping.store.service;

import org.springframework.data.domain.Page;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductCategory;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductDto;
import ru.yandex.practicum.commerce.dto.shopping.store.SetProductQuantityStateRequest;

import java.util.UUID;

public interface ShoppingStoreService {

    Page<ProductDto> getProducts(ProductCategory category, int page, int size, String sort);

    ProductDto createProduct(ProductDto productDto);

    ProductDto updateProduct(ProductDto productDto);

    boolean removeProductFromStore(UUID productId);

    boolean setProductQuantityState(SetProductQuantityStateRequest request);

    ProductDto getProduct(UUID productId);
}
