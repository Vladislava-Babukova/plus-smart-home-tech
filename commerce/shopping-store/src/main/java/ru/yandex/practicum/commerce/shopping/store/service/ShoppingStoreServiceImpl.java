package ru.yandex.practicum.commerce.shopping.store.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductCategory;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductDto;
import ru.yandex.practicum.commerce.dto.shopping.store.ProductState;
import ru.yandex.practicum.commerce.shopping.store.dal.ProductRepository;
import ru.yandex.practicum.commerce.shopping.store.exception.ProductNotFoundBusinessException;
import ru.yandex.practicum.commerce.shopping.store.mapper.ProductMapper;
import ru.yandex.practicum.commerce.shopping.store.model.ProductEntity;
import ru.yandex.practicum.commerce.dto.shopping.store.SetProductQuantityStateRequest;

import java.util.*;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ShoppingStoreServiceImpl implements ShoppingStoreService {

    private final ProductRepository productRepository;

    @Override
    public Page<ProductDto> getProducts(ProductCategory category, int page, int size, String sort) {
        log.debug("Getting products for category: {}, page: {}, size: {}, sort: {}",
                category, page, size, sort);

        Pageable pageable = createPageable(page, size, sort);
        Page<ProductEntity> productPage = productRepository.findByProductCategory(category, pageable);
        log.debug("Found {} products in category {}", productPage.getTotalElements(), category);
        return productPage.map(ProductMapper::toDto);
    }

    @Override
    @Transactional
    public ProductDto createProduct(ProductDto productDto) {
        log.debug("Creating new product: {}", productDto.getProductName());

        ProductEntity productEntity = ProductMapper.toEntity(productDto);
        ProductEntity savedEntity = productRepository.save(productEntity);

        log.info("Created new product with id: {}", savedEntity.getProductId());
        return ProductMapper.toDto(savedEntity);
    }

    @Override
    @Transactional
    public ProductDto updateProduct(ProductDto productDto) {
        log.debug("Updating product with id: {}", productDto.getProductId());

        UUID productId = productDto.getProductId();
        ProductEntity existingProduct = ifProductExistedInDb(productId);

        ProductMapper.updateEntityFromDto(existingProduct, productDto);
        ProductEntity updatedProduct = productRepository.save(existingProduct);

        log.info("Updated product with id: {}", productId);
        return ProductMapper.toDto(updatedProduct);
    }

    @Override
    @Transactional
    public boolean removeProductFromStore(UUID productId) {
        log.debug("Removing product from store with id: {}", productId);

        ProductEntity product = ifProductExistedInDb(productId);

        product.setProductState(ProductState.DEACTIVATE);
        productRepository.save(product);

        log.info("Product with id: {} deactivated", productId);
        return true;
    }

    @Override
    @Transactional
    public boolean setProductQuantityState(SetProductQuantityStateRequest request) {
        log.debug("Setting quantity state for product: {} to {}",
                request.getProductId(), request.getQuantityState());

        ProductEntity product = ifProductExistedInDb(request.getProductId());

        product.setQuantityState(request.getQuantityState());
        productRepository.save(product);

        log.info("Quantity state updated for product: {} to {}",
                request.getProductId(), request.getQuantityState());
        return true;
    }

    @Override
    public ProductDto getProduct(UUID productId) {
        log.debug("Getting product with id: {}", productId);

        ProductEntity product = ifProductExistedInDb(productId);

        return ProductMapper.toDto(product);
    }

    private Pageable createPageable(int page, int size, String sort) {
        if (sort == null || sort.trim().isEmpty()) {
            return PageRequest.of(page, size);
        }

        if (sort.contains(",")) {
            String[] parts = sort.split(",");
            if (parts.length == 2) {
                String property = parts[0].trim();
                String directionStr = parts[1].trim();

                Sort.Direction direction;
                try {
                    direction = Sort.Direction.fromString(directionStr);
                } catch (IllegalArgumentException e) {
                    direction = Sort.Direction.ASC;
                }

                return PageRequest.of(page, size, Sort.by(direction, property));
            }
        }

        return PageRequest.of(page, size, Sort.by(Sort.Direction.ASC, sort.trim()));
    }

    private ProductEntity ifProductExistedInDb(UUID productId) {
        return productRepository.findById(productId)
                .orElseThrow(() -> new ProductNotFoundBusinessException(productId));
    }
}
