package ru.yandex.practicum.commerce.contract.shopping.store;

import org.springframework.cloud.openfeign.FeignClient;

@FeignClient(name = "shopping-store")
public interface ShoppingStoreClient extends ShoppingStoreOperations {
}