package ru.yandex.practicum.commerce.contract.shopping.store;

import ru.yandex.practicum.commerce.dto.shopping.store.ProductCategory;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductDto;
import org.springframework.data.domain.Page;
import ru.yandex.practicum.commerce.dto.shopping.store.QuantityState;

import java.util.UUID;

public interface ShoppingStoreOperations {

    Page<ProductDto> getProducts(ProductCategory category, int page, int size, String sort);

    ProductDto createNewProduct(ProductDto productDto);

    ProductDto updateProduct(ProductDto productDto);

    boolean removeProductFromStore(UUID productId);

    boolean setProductQuantityState(UUID productId, QuantityState quantityState);

    ProductDto getProduct(UUID productId);
}
