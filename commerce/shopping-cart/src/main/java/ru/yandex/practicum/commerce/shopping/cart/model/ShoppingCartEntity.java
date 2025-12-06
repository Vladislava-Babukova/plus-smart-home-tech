package ru.yandex.practicum.commerce.shopping.cart.model;

import jakarta.persistence.*;
import lombok.*;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "shopping_carts")
@Setter
@Getter
@EqualsAndHashCode(of = {"shoppingCartId"})
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShoppingCartEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "shopping_cart_id")
    private UUID shoppingCartId;

    @Column(name = "username", nullable = false, length = 100)
    private String username;

    @Enumerated(EnumType.STRING)
    @Column(name = "cart_state", nullable = false)
    private ShoppingCartState cartState;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (cartState == null) {
            cartState = ShoppingCartState.ACTIVE;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
