package ru.yandex.practicum.commerce.contract.shopping.cart;

import org.springframework.cloud.openfeign.FallbackFactory;
import ru.yandex.practicum.commerce.dto.shopping.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.dto.shopping.cart.ShoppingCartDto;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class ShoppingCartClientFallbackFactory implements FallbackFactory<ShoppingCartClient> {
    @Override
    public ShoppingCartClient create(Throwable cause) {
        return new ShoppingCartClient() {

            @Override
            public ShoppingCartDto getShoppingCart(String username) {
                throw new RuntimeException("Shopping-cart service unavailable", cause);
            }

            @Override
            public ShoppingCartDto addProductToShoppingCart(String username, Map<UUID, Integer> products) {
                throw new RuntimeException("Shopping-cart service unavailable", cause);
            }

            @Override
            public void deactivateCurrentShoppingCart(String username) {
                throw new RuntimeException("Shopping-cart service unavailable", cause);
            }

            @Override
            public ShoppingCartDto removeFromShoppingCart(String username, List<UUID> products) {
                throw new RuntimeException("Shopping-cart service unavailable", cause);
            }

            @Override
            public ShoppingCartDto changeProductQuantity(String username, ChangeProductQuantityRequest request) {
                throw new RuntimeException("Shopping-cart service unavailable", cause);
            }
        };
    }
}
