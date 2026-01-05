package ru.yandex.practicum.commerce.payment.dal;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import ru.yandex.practicum.commerce.payment.model.PaymentEntity;
import ru.yandex.practicum.commerce.payment.model.PaymentState;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface PaymentRepository extends JpaRepository<PaymentEntity, UUID> {

    Optional<PaymentEntity> findByOrderId(UUID orderId);

    List<PaymentEntity> findByPaymentState(PaymentState state);

    @Query("SELECT p FROM PaymentEntity p WHERE p.orderId = :orderId AND p.paymentState = :state")
    Optional<PaymentEntity> findByOrderIdAndPaymentState(@Param("orderId") UUID orderId,
                                                         @Param("state") PaymentState state);

    boolean existsByPaymentIdAndPaymentState(UUID paymentId, PaymentState state);
}
