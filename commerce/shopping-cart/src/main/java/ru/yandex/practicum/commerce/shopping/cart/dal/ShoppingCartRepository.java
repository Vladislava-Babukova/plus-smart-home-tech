package ru.yandex.practicum.commerce.shopping.cart.dal;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.commerce.shopping.cart.model.ShoppingCartEntity;
import ru.yandex.practicum.commerce.shopping.cart.model.ShoppingCartState;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface ShoppingCartRepository extends JpaRepository<ShoppingCartEntity, UUID> {

    Optional<ShoppingCartEntity> findByUsernameAndCartState(String username, ShoppingCartState cartState);

    @Query("SELECT sc FROM ShoppingCartEntity sc WHERE sc.username = :username AND sc.cartState = 'ACTIVE'")
    Optional<ShoppingCartEntity> findActiveCartByUsername(@Param("username") String username);

    boolean existsByUsernameAndCartState(String username, ShoppingCartState cartState);
}
