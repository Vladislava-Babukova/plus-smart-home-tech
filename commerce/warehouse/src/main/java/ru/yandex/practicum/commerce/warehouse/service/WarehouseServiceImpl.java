package ru.yandex.practicum.commerce.warehouse.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.dto.shopping.cart.ShoppingCartDto;
import ru.yandex.practicum.commerce.dto.warehouse.AddProductToWarehouseRequest;
import ru.yandex.practicum.commerce.dto.warehouse.AddressDto;
import ru.yandex.practicum.commerce.dto.warehouse.BookedProductsDto;
import ru.yandex.practicum.commerce.dto.warehouse.NewProductInWarehouseRequest;
import ru.yandex.practicum.commerce.warehouse.dal.WarehouseProductRepository;
import ru.yandex.practicum.commerce.warehouse.exception.NoSpecifiedProductInWarehouseBusinessException;
import ru.yandex.practicum.commerce.warehouse.exception.ProductInShoppingCartLowQuantityBusinessException;
import ru.yandex.practicum.commerce.warehouse.exception.SpecifiedProductAlreadyInWarehouseBusinessException;
import ru.yandex.practicum.commerce.warehouse.mapper.WarehouseMapper;
import ru.yandex.practicum.commerce.warehouse.model.WarehouseProductEntity;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class WarehouseServiceImpl implements WarehouseService {

    private final WarehouseProductRepository warehouseProductRepository;

    private static final String[] ADDRESSES = new String[]{"ADDRESS_1", "ADDRESS_2"};
    private static final String CURRENT_ADDRESS = ADDRESSES[Random.from(new SecureRandom()).nextInt(0, ADDRESSES.length)];

    @Override
    @Transactional
    public void newProductInWarehouse(NewProductInWarehouseRequest request) {
        log.debug("Adding new product to warehouse: {}", request.getProductId());

        if (warehouseProductRepository.existsByProductId(request.getProductId())) {
            throw new SpecifiedProductAlreadyInWarehouseBusinessException(request.getProductId());
        }

        WarehouseProductEntity entity = WarehouseMapper.toEntity(request);
        warehouseProductRepository.save(entity);

        log.info("New product added to warehouse: {}", request.getProductId());
    }

    @Override
    public BookedProductsDto checkProductQuantityEnoughForShoppingCart(ShoppingCartDto shoppingCartDto) {
        log.debug("Checking product quantity for shopping cart: {}", shoppingCartDto.getShoppingCartId());

        Map<UUID, Integer> insufficientProducts = new HashMap<>();
        double totalWeight = 0.0;
        double totalVolume = 0.0;
        boolean hasFragile = false;

        for (Map.Entry<UUID, Integer> entry : shoppingCartDto.getProducts().entrySet()) {
            UUID productId = entry.getKey();
            Integer requiredQuantity = entry.getValue();

            WarehouseProductEntity product = warehouseProductRepository.findByProductId(productId)
                    .orElseThrow(() -> new NoSpecifiedProductInWarehouseBusinessException(productId));

            if (product.getQuantity() < requiredQuantity) {
                insufficientProducts.put(productId, requiredQuantity - product.getQuantity().intValue());
            }

            if (product.getWeight() != null) {
                totalWeight += product.getWeight() * requiredQuantity;
            }
            if (product.getWidth() != null && product.getHeight() != null && product.getDepth() != null) {
                totalVolume += product.getWidth() * product.getHeight() * product.getDepth() * requiredQuantity;
            }
            if (Boolean.TRUE.equals(product.getFragile())) {
                hasFragile = true;
            }
        }

        if (!insufficientProducts.isEmpty()) {
            throw new ProductInShoppingCartLowQuantityBusinessException(insufficientProducts);
        }

        BookedProductsDto result = BookedProductsDto.builder()
                .deliveryWeight(totalWeight)
                .deliveryVolume(totalVolume)
                .fragile(hasFragile)
                .build();

        log.debug("Product quantity check completed for cart: {}, result: {}", shoppingCartDto.getShoppingCartId(), result);
        return result;
    }

    @Override
    @Transactional
    public void addProductToWarehouse(AddProductToWarehouseRequest request) {
        log.debug("Adding product quantity to warehouse: {}", request.getProductId());

        WarehouseProductEntity product = warehouseProductRepository.findByProductId(request.getProductId())
                .orElseThrow(() -> new NoSpecifiedProductInWarehouseBusinessException(request.getProductId()));

        product.setQuantity(product.getQuantity() + request.getQuantity());
        warehouseProductRepository.save(product);

        log.info("Product quantity updated for: {}, new quantity: {}", request.getProductId(), product.getQuantity());
    }

    @Override
    public AddressDto getWarehouseAddress() {
        log.debug("Getting warehouse address: {}", CURRENT_ADDRESS);

        return AddressDto.builder()
                .country(CURRENT_ADDRESS)
                .city(CURRENT_ADDRESS)
                .street(CURRENT_ADDRESS)
                .house(CURRENT_ADDRESS)
                .flat(CURRENT_ADDRESS)
                .build();
    }
}
