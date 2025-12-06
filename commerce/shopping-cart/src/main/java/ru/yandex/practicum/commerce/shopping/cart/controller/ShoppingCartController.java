package ru.yandex.practicum.commerce.shopping.cart.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.contract.shopping.cart.ShoppingCartOperations;
import ru.yandex.practicum.commerce.dto.shopping.cart.ChangeProductQuantityRequest;
import ru.yandex.practicum.commerce.dto.shopping.cart.ShoppingCartDto;
import ru.yandex.practicum.commerce.shopping.cart.service.ShoppingCartService;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/shopping-cart")
@RequiredArgsConstructor
@Validated
public class ShoppingCartController implements ShoppingCartOperations {

    private final ShoppingCartService shoppingCartService;

    @Override
    @GetMapping
    public ShoppingCartDto getShoppingCart(@RequestParam String username) {
        log.debug("Requested shopping cart for user: {}", username);
        ShoppingCartDto shoppingCart = shoppingCartService.getShoppingCart(username);
        log.debug("Return shopping cart: {}", shoppingCart);
        return shoppingCart;
    }

    @Override
    @PutMapping
    public ShoppingCartDto addProductToShoppingCart(@RequestParam String username,
                                                    @RequestBody Map<UUID, Integer> products) {
        log.debug("Adding products to cart for user: {}, products: {}", username, products);
        ShoppingCartDto shoppingCart = shoppingCartService.addProductToShoppingCart(username, products);
        log.debug("Return updated shopping cart: {}", shoppingCart);
        return shoppingCart;
    }

    @Override
    @DeleteMapping
    public void deactivateCurrentShoppingCart(@RequestParam String username) {
        log.debug("Deactivating shopping cart for user: {}", username);
        shoppingCartService.deactivateCurrentShoppingCart(username);
        log.debug("Shopping cart deactivated for user: {}", username);
    }

    @Override
    @PostMapping("/remove")
    public ShoppingCartDto removeFromShoppingCart(@RequestParam String username,
                                                  @RequestBody List<UUID> productIds) {
        log.debug("Removing products from cart for user: {}, productIds: {}", username, productIds);
        ShoppingCartDto shoppingCart = shoppingCartService.removeFromShoppingCart(username, productIds);
        log.debug("Return updated shopping cart: {}", shoppingCart);
        return shoppingCart;
    }

    @Override
    @PostMapping("/change-quantity")
    public ShoppingCartDto changeProductQuantity(@RequestParam String username,
                                                 @RequestBody ChangeProductQuantityRequest request) {
        log.debug("Changing product quantity for user: {}, request: {}", username, request);
        ShoppingCartDto shoppingCart = shoppingCartService.changeProductQuantity(username, request);
        log.debug("Return updated shopping cart: {}", shoppingCart);
        return shoppingCart;
    }
}
