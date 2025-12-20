package ru.yandex.practicum.commerce.contract.warehouse;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.commerce.dto.shopping.cart.ShoppingCartDto;
import ru.yandex.practicum.commerce.dto.warehouse.*;

import java.util.Map;
import java.util.UUID;

@FeignClient(name = "warehouse", fallback = WarehouseClientFallback.class)
public interface WarehouseClient extends WarehouseOperations{

    @PutMapping("/api/v1/warehouse")
    void newProductInWarehouse(@RequestBody NewProductInWarehouseRequest request);

    @PostMapping("/api/v1/warehouse/check")
    BookedProductsDto checkProductQuantityEnoughForShoppingCart(@RequestBody ShoppingCartDto shoppingCartDto);

    @PostMapping("/api/v1/warehouse/add")
    void addProductToWarehouse(@RequestBody AddProductToWarehouseRequest request);

    @GetMapping("/api/v1/warehouse/address")
    AddressDto getWarehouseAddress();

    @PostMapping("/assembly")
    BookedProductsDto assemblyProductsForOrder(@RequestBody AssemblyProductsForOrderRequest request);

    @PostMapping("/shipped")
    void shippedToDelivery(@RequestBody ShippedToDeliveryRequest request);

    @PostMapping("/return")
    void acceptReturn(@RequestBody Map<UUID, Integer> returnedProducts);
}