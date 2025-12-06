package ru.yandex.practicum.commerce.contract.warehouse;

import org.springframework.stereotype.Component;
import ru.yandex.practicum.commerce.dto.shopping.cart.ShoppingCartDto;
import ru.yandex.practicum.commerce.dto.warehouse.BookedProductsDto;

@Component
public class WarehouseClientFallback  implements WarehouseClient {
    @Override
    public BookedProductsDto checkProductQuantityEnoughForShoppingCart(ShoppingCartDto shoppingCartDto) {
        throw new RuntimeException("Warehouse service is unavailable");
    }
}
