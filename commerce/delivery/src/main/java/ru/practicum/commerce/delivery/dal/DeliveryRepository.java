package ru.practicum.commerce.delivery.dal;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.practicum.commerce.delivery.model.DeliveryEntity;
import ru.yandex.practicum.commerce.dto.delivery.DeliveryState;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface DeliveryRepository extends JpaRepository<DeliveryEntity, UUID> {

    Optional<DeliveryEntity> findByOrderId(UUID orderId);

    List<DeliveryEntity> findByDeliveryState(DeliveryState state);

    @Query("SELECT d FROM DeliveryEntity d WHERE d.orderId = :orderId AND d.deliveryState = :state")
    Optional<DeliveryEntity> findByOrderIdAndDeliveryState(@Param("orderId") UUID orderId,
                                                           @Param("state") DeliveryState state);

    boolean existsByDeliveryIdAndDeliveryState(UUID deliveryId, DeliveryState state);

    @Query("SELECT d FROM DeliveryEntity d WHERE d.toAddress.city = :city")
    List<DeliveryEntity> findByCity(@Param("city") String city);
}
