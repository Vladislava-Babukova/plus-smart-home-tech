package ru.practicum.commerce.delivery.model;

import jakarta.persistence.*;
import lombok.*;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryState;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "deliveries")
@Setter
@Getter
@EqualsAndHashCode(of = {"deliveryId"})
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class DeliveryEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    @Column(name = "delivery_id")
    private UUID deliveryId;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    @Enumerated(EnumType.STRING)
    @Column(name = "delivery_state", nullable = false)
    private DeliveryState deliveryState;

    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "country", column = @Column(name = "from_country")),
            @AttributeOverride(name = "city", column = @Column(name = "from_city")),
            @AttributeOverride(name = "street", column = @Column(name = "from_street")),
            @AttributeOverride(name = "house", column = @Column(name = "from_house")),
            @AttributeOverride(name = "flat", column = @Column(name = "from_flat"))
    })
    private Address fromAddress;

    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "country", column = @Column(name = "to_country")),
            @AttributeOverride(name = "city", column = @Column(name = "to_city")),
            @AttributeOverride(name = "street", column = @Column(name = "to_street")),
            @AttributeOverride(name = "house", column = @Column(name = "to_house")),
            @AttributeOverride(name = "flat", column = @Column(name = "to_flat"))
    })
    private Address toAddress;

    @Column(name = "delivery_weight")
    private Double deliveryWeight;

    @Column(name = "delivery_volume")
    private Double deliveryVolume;

    @Column(name = "fragile")
    private Boolean fragile;

    @Column(name = "delivery_cost", precision = 10, scale = 2)
    private BigDecimal deliveryCost;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @Version
    private Long version;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        updatedAt = LocalDateTime.now();
        if (deliveryState == null) {
            deliveryState = DeliveryState.CREATED;
        }
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }


    @Embeddable
    @Setter
    @Getter
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Address {
        @Column(name = "country")
        private String country;

        @Column(name = "city")
        private String city;

        @Column(name = "street")
        private String street;

        @Column(name = "house")
        private String house;

        @Column(name = "flat")
        private String flat;
    }
}
