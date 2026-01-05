package ru.yandex.practicum.commerce.warehouse.controller;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;
import ru.yandex.practicum.commerce.contract.warehouse.WarehouseOperations;
import ru.yandex.practicum.commerce.dto.shopping.cart.ShoppingCartDto;
import ru.yandex.practicum.commerce.dto.warehouse.*;
import ru.yandex.practicum.commerce.warehouse.service.WarehouseService;

import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/v1/warehouse")
@RequiredArgsConstructor
public class WarehouseController implements WarehouseOperations {

    private final WarehouseService warehouseService;

    @Override
    @PutMapping
    public void newProductInWarehouse(@RequestBody NewProductInWarehouseRequest request) {
        log.debug("Adding new product to warehouse: {}", request);
        warehouseService.newProductInWarehouse(request);
        log.debug("New product added to warehouse successfully");
    }

    @Override
    @PostMapping("/check")
    public BookedProductsDto checkProductQuantityEnoughForShoppingCart(@RequestBody ShoppingCartDto shoppingCartDto) {
        log.debug("Checking product quantity for shopping cart: {}", shoppingCartDto);
        BookedProductsDto result = warehouseService.checkProductQuantityEnoughForShoppingCart(shoppingCartDto);
        log.debug("Product quantity check completed: {}", result);
        return result;
    }

    @Override
    @PostMapping("/add")
    public void addProductToWarehouse(@RequestBody AddProductToWarehouseRequest request) {
        log.debug("Adding product quantity to warehouse: {}", request);
        warehouseService.addProductToWarehouse(request);
        log.debug("Product quantity added to warehouse successfully");
    }

    @Override
    @GetMapping("/address")
    public AddressDto getWarehouseAddress() {
        log.debug("Requesting warehouse address");
        AddressDto address = warehouseService.getWarehouseAddress();
        log.debug("Return warehouse address: {}", address);
        return address;
    }

    @Override
    @PostMapping("/assembly")
    public BookedProductsDto assemblyProductsForOrder(@RequestBody AssemblyProductsForOrderRequest request) {
        log.debug("Assembling products for order: {}", request.getOrderId());
        BookedProductsDto result = warehouseService.assemblyProductsForOrder(request);
        log.debug("Products assembled for order: {}", request.getOrderId());
        return result;
    }

    @Override
    @PostMapping("/shipped")
    public void shippedToDelivery(@RequestBody ShippedToDeliveryRequest request) {
        log.debug("Shipping products to delivery for order: {}", request.getOrderId());
        warehouseService.shippedToDelivery(request);
        log.debug("Products shipped to delivery for order: {}", request.getOrderId());
    }

    @Override
    @PostMapping("/return")
    public void acceptReturn(@RequestBody Map<UUID, Integer> returnedProducts) {
        log.debug("Accepting product returns: {}", returnedProducts);
        warehouseService.acceptReturn(returnedProducts);
        log.debug("Product returns accepted successfully");
    }
}