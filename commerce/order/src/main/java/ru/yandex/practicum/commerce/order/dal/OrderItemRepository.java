package ru.yandex.practicum.commerce.order.dal;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.commerce.order.model.OrderItemEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface OrderItemRepository extends JpaRepository<OrderItemEntity, UUID> {
    List<OrderItemEntity> findByOrderOrderId(UUID orderId);

    @Modifying
    @Query("DELETE FROM OrderItemEntity oi WHERE oi.order.orderId = :orderId")
    void deleteByOrderId(@Param("orderId") UUID orderId);

    @Query("SELECT oi FROM OrderItemEntity oi WHERE oi.order.orderId = :orderId AND oi.productId = :productId")
    Optional<OrderItemEntity> findByOrderIdAndProductId(@Param("orderId") UUID orderId,
                                                        @Param("productId") UUID productId);
}
