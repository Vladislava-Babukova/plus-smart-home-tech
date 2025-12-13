package ru.yandex.practicum.commerce.shopping.cart.model;

import jakarta.persistence.*;
import lombok.*;

import java.util.UUID;

@Entity
@Table(name = "shopping_cart_items")
@Setter
@Getter
@EqualsAndHashCode(of = {"cartItemId"})
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ShoppingCartItemEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "cart_item_id")
    private UUID cartItemId;

    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "shopping_cart_id", nullable = false)
    private ShoppingCartEntity shoppingCart;

    @Column(name = "product_id", nullable = false)
    private UUID productId;

    @Column(name = "quantity", nullable = false)
    private Integer quantity;

    @Version
    private Long version;
}
