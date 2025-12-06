package ru.yandex.practicum.commerce.warehouse.dal;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.commerce.warehouse.model.WarehouseProductEntity;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface WarehouseProductRepository extends JpaRepository<WarehouseProductEntity, UUID> {

    Optional<WarehouseProductEntity> findByProductId(UUID productId);

    List<WarehouseProductEntity> findByProductIdIn(List<UUID> productIds);

    @Query("SELECT wp FROM WarehouseProductEntity wp WHERE wp.productId IN :productIds AND wp.quantity > 0")
    List<WarehouseProductEntity> findAvailableProductsByIds(@Param("productIds") List<UUID> productIds);

    boolean existsByProductId(UUID productId);
}
