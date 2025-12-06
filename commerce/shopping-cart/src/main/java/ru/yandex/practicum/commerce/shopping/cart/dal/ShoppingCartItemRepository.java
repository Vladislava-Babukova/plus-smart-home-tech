package ru.yandex.practicum.commerce.shopping.cart.dal;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.commerce.shopping.cart.model.ShoppingCartItemEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface ShoppingCartItemRepository extends JpaRepository<ShoppingCartItemEntity, UUID> {

    List<ShoppingCartItemEntity> findByShoppingCart_ShoppingCartId(UUID shoppingCartId);

    Optional<ShoppingCartItemEntity> findByShoppingCart_ShoppingCartIdAndProductId(UUID shoppingCartId, UUID productId);

    List<ShoppingCartItemEntity> findByShoppingCart_ShoppingCartIdAndProductIdIn(UUID shoppingCartId, List<UUID> productIds);

    @Modifying
    @Query("DELETE FROM ShoppingCartItemEntity sci WHERE sci.shoppingCart.shoppingCartId = :shoppingCartId AND sci.productId IN :productIds")
    void deleteByShoppingCartIdAndProductIds(@Param("shoppingCartId") UUID shoppingCartId,
                                             @Param("productIds") List<UUID> productIds);

    @Modifying
    @Query("DELETE FROM ShoppingCartItemEntity sci WHERE sci.shoppingCart.shoppingCartId = :shoppingCartId")
    void deleteByShoppingCartId(@Param("shoppingCartId") UUID shoppingCartId);
}
