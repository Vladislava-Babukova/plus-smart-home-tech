package ru.yandex.practicum.commerce.warehouse.dal;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.commerce.warehouse.model.OrderBookingEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OrderBookingRepository extends JpaRepository<OrderBookingEntity, UUID> {

    List<OrderBookingEntity> findByOrderId(UUID orderId);

    Optional<OrderBookingEntity> findByOrderIdAndProductId(UUID orderId, UUID productId);

    List<OrderBookingEntity> findByDeliveryId(UUID deliveryId);

    @Modifying
    @Query("UPDATE OrderBookingEntity ob SET ob.deliveryId = :deliveryId WHERE ob.orderId = :orderId")
    void updateDeliveryIdByOrderId(@Param("orderId") UUID orderId, @Param("deliveryId") UUID deliveryId);

    boolean existsByOrderIdAndProductId(UUID orderId, UUID productId);

    @Query("SELECT SUM(ob.quantity) FROM OrderBookingEntity ob WHERE ob.productId = :productId")
    Long getTotalBookedQuantityByProductId(@Param("productId") UUID productId);
}
