package ru.practicum.commerce.delivery.exception;

import java.util.UUID;

public class NoDeliveryFoundBusinessException extends RuntimeException {
    private final UUID deliveryId;

    public NoDeliveryFoundBusinessException(UUID deliveryId) {
        super("Delivery not found with id: " + deliveryId);
        this.deliveryId = deliveryId;
    }

    public NoDeliveryFoundBusinessException(UUID deliveryId, String message) {
        super(message);
        this.deliveryId = deliveryId;
    }

    public UUID getDeliveryId() {
        return deliveryId;
    }
}
