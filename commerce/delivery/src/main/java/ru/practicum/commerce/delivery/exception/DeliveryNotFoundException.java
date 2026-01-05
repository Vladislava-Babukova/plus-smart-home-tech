package ru.practicum.commerce.delivery.exception;

import java.util.UUID;

public class DeliveryNotFoundException extends RuntimeException {
    public DeliveryNotFoundException(UUID deliveryId) {
        super("Delivery not found with id: " + deliveryId);
    }

    public DeliveryNotFoundException(String message) {
        super(message);
    }
}