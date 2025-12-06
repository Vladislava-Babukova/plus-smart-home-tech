package ru.yandex.practicum.commerce.shopping.store.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.shopping.store.config.Loggable;
import ru.yandex.practicum.commerce.contract.shopping.store.ShoppingStoreOperations;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductCategory;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductDto;
import ru.yandex.practicum.commerce.dto.shopping.store.QuantityState;
import ru.yandex.practicum.commerce.dto.shopping.store.SetProductQuantityStateRequest;
import ru.yandex.practicum.commerce.shopping.store.service.ShoppingStoreService;

import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-store")
@RequiredArgsConstructor
@Validated
public class ShoppingStoreController implements ShoppingStoreOperations {
    private final ShoppingStoreService shoppingStoreService;

    @Override
    @GetMapping
    @Loggable
    public Page<ProductDto> getProducts(
            @RequestParam ProductCategory category,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size,
            @RequestParam(required = false) String sort) {

        Page<ProductDto> productDtos = shoppingStoreService.getProducts(category, page, size, sort);
        return productDtos;
    }

    @Override
    @PutMapping
    @Loggable
    public ProductDto createNewProduct(@RequestBody ProductDto productDto) {
        ProductDto newProduct = shoppingStoreService.createProduct(productDto);
        return newProduct;
    }

    @Override
    @PostMapping
    @Loggable
    public ProductDto updateProduct(@RequestBody ProductDto productDto) {
        ProductDto updatedProduct = shoppingStoreService.updateProduct(productDto);
        return updatedProduct;
    }

    @Override
    @PostMapping("/removeProductFromStore")
    @Loggable
    public boolean removeProductFromStore(@RequestBody UUID productId) {
        return shoppingStoreService.removeProductFromStore(productId);
    }

    @Override
    @PostMapping("/quantityState")
    @Loggable
    public boolean setProductQuantityState(
            @RequestParam UUID productId,
            @RequestParam QuantityState quantityState) {
        SetProductQuantityStateRequest request = SetProductQuantityStateRequest.builder()
                .productId(productId)
                .quantityState(quantityState)
                .build();
        return shoppingStoreService.setProductQuantityState(request);
    }

    @Override
    @GetMapping("/{productId}")
    @Loggable
    public ProductDto getProduct(@PathVariable UUID productId) {
        ProductDto productDto = shoppingStoreService.getProduct(productId);
        return productDto;
    }
}
