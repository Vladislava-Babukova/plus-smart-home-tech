package ru.yandex.practicum.commerce.shopping.cart.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.commerce.contract.warehouse.WarehouseClient;
import ru.yandex.practicum.commerce.dto.shopping.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.dto.shopping.cart.ShoppingCartDto;
import ru.yandex.practicum.commerce.shopping.cart.dal.ShoppingCartItemRepository;
import ru.yandex.practicum.commerce.shopping.cart.dal.ShoppingCartRepository;
import ru.yandex.practicum.commerce.shopping.cart.exception.NoProductsInShoppingCartBusinessException;
import ru.yandex.practicum.commerce.shopping.cart.exception.NotAuthorizedBusinessException;
import ru.yandex.practicum.commerce.shopping.cart.mapper.ShoppingCartMapper;
import ru.yandex.practicum.commerce.shopping.cart.model.ShoppingCartEntity;
import ru.yandex.practicum.commerce.shopping.cart.model.ShoppingCartItemEntity;
import ru.yandex.practicum.commerce.shopping.cart.model.ShoppingCartState;

import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ShoppingCartServiceImpl implements ShoppingCartService {

    private final ShoppingCartRepository shoppingCartRepository;
    private final ShoppingCartItemRepository shoppingCartItemRepository;
    private final WarehouseClient warehouseClient;

    @Override
    public ShoppingCartDto getShoppingCart(String username) {
        validateUsername(username);
        log.debug("Getting shopping cart for user: {}", username);
        ShoppingCartEntity shoppingCart = getOrCreateActiveCart(username);
        List<ShoppingCartItemEntity> items = shoppingCartItemRepository
                .findByShoppingCart_ShoppingCartId(shoppingCart.getShoppingCartId());

        return ShoppingCartMapper.toDto(shoppingCart, items);
    }

    @Override
    @Transactional
    public ShoppingCartDto addProductToShoppingCart(String username, Map<UUID, Integer> products) {
        validateUsername(username);
        log.debug("Adding products to cart for user: {}, products: {}", username, products);
        ShoppingCartEntity shoppingCart = getOrCreateActiveCart(username);

        ShoppingCartDto tempCart = ShoppingCartDto.builder()
                .shoppingCartId(shoppingCart.getShoppingCartId())
                .products(new HashMap<>(products))
                .build();

        try {
            warehouseClient.checkProductQuantityEnoughForShoppingCart(tempCart);
            log.debug("Products availability confirmed by warehouse");
        } catch (Exception e) {
            log.error("Failed to check product availability in warehouse: {}", e.getMessage());
            throw new RuntimeException("Product availability check failed: " + e.getMessage(), e);
        }

        for (Map.Entry<UUID, Integer> entry : products.entrySet()) {
            UUID productId = entry.getKey();
            Integer quantity = entry.getValue();

            Optional<ShoppingCartItemEntity> existingItem = shoppingCartItemRepository
                    .findByShoppingCart_ShoppingCartIdAndProductId(shoppingCart.getShoppingCartId(), productId);

            if (existingItem.isPresent()) {
                ShoppingCartItemEntity item = existingItem.get();
                item.setQuantity(item.getQuantity() + quantity);
                shoppingCartItemRepository.save(item);
                log.debug("Updated quantity for product {} in cart: {}", productId, item.getQuantity());
            } else {
                ShoppingCartItemEntity newItem = ShoppingCartMapper.toNewItemEntity(shoppingCart, productId, quantity);
                shoppingCartItemRepository.save(newItem);
                log.debug("Added new product {} to cart with quantity: {}", productId, quantity);
            }
        }

        List<ShoppingCartItemEntity> updatedItems = shoppingCartItemRepository
                .findByShoppingCart_ShoppingCartId(shoppingCart.getShoppingCartId());

        return ShoppingCartMapper.toDto(shoppingCart, updatedItems);
    }

    @Override
    @Transactional
    public void deactivateCurrentShoppingCart(String username) {
        validateUsername(username);
        log.debug("Deactivating shopping cart for user: {}", username);

        Optional<ShoppingCartEntity> activeCart = shoppingCartRepository
                .findByUsernameAndCartState(username, ShoppingCartState.ACTIVE);

        if (activeCart.isPresent()) {
            ShoppingCartEntity cart = activeCart.get();
            cart.setCartState(ShoppingCartState.DEACTIVATED);
            shoppingCartRepository.save(cart);
            log.info("Shopping cart deactivated for user: {}", username);
        } else {
            log.warn("No active shopping cart found for user: {}", username);
        }
    }

    @Override
    @Transactional
    public ShoppingCartDto removeFromShoppingCart(String username, List<UUID> productIds) {
        validateUsername(username);
        log.debug("Removing products from cart for user: {}, productIds: {}", username, productIds);

        ShoppingCartEntity shoppingCart = getActiveCartOrThrow(username);

        List<ShoppingCartItemEntity> existingItems = shoppingCartItemRepository
                .findByShoppingCart_ShoppingCartIdAndProductIdIn(shoppingCart.getShoppingCartId(), productIds);

        if (existingItems.size() != productIds.size()) {
            List<UUID> foundProductIds = existingItems.stream()
                    .map(ShoppingCartItemEntity::getProductId)
                    .collect(Collectors.toList());

            List<UUID> missingProductIds = productIds.stream()
                    .filter(id -> !foundProductIds.contains(id))
                    .collect(Collectors.toList());

            throw new NoProductsInShoppingCartBusinessException(missingProductIds);
        }

        shoppingCartItemRepository.deleteByShoppingCartIdAndProductIds(shoppingCart.getShoppingCartId(), productIds);
        log.debug("Removed {} products from cart for user: {}", productIds.size(), username);

        List<ShoppingCartItemEntity> remainingItems = shoppingCartItemRepository
                .findByShoppingCart_ShoppingCartId(shoppingCart.getShoppingCartId());

        return ShoppingCartMapper.toDto(shoppingCart, remainingItems);
    }

    @Override
    @Transactional
    public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
        validateUsername(username);
        log.debug("Changing product quantity for user: {}, request: {}", username, request);

        ShoppingCartEntity shoppingCart = getActiveCartOrThrow(username);

        Optional<ShoppingCartItemEntity> existingItem = shoppingCartItemRepository
                .findByShoppingCart_ShoppingCartIdAndProductId(shoppingCart.getShoppingCartId(), request.getProductId());

        if (existingItem.isEmpty()) {
            throw new NoProductsInShoppingCartBusinessException(List.of(request.getProductId()));
        }

        ShoppingCartItemEntity item = existingItem.get();
        item.setQuantity(request.getNewQuantity().intValue());
        shoppingCartItemRepository.save(item);

        log.debug("Changed quantity for product {} to {}", request.getProductId(), request.getNewQuantity());

        List<ShoppingCartItemEntity> updatedItems = shoppingCartItemRepository
                .findByShoppingCart_ShoppingCartId(shoppingCart.getShoppingCartId());

        return ShoppingCartMapper.toDto(shoppingCart, updatedItems);
    }

    private void validateUsername(String username) {
        if (username == null || username.trim().isEmpty()) {
            throw new NotAuthorizedBusinessException("Username cannot be empty");
        }
    }

    private ShoppingCartEntity getOrCreateActiveCart(String username) {
        return shoppingCartRepository.findActiveCartByUsername(username)
                .orElseGet(() -> {
                    ShoppingCartEntity newCart = ShoppingCartMapper.toNewEntity(username);
                    return shoppingCartRepository.save(newCart);
                });
    }

    private ShoppingCartEntity getActiveCartOrThrow(String username) {
        return shoppingCartRepository.findActiveCartByUsername(username)
                .orElseThrow(() -> new NotAuthorizedBusinessException("No active shopping cart found for user: " + username));
    }
}