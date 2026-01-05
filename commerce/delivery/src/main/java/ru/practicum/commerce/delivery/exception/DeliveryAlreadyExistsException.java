package ru.practicum.commerce.delivery.exception;

public class DeliveryAlreadyExistsException extends RuntimeException {
    public DeliveryAlreadyExistsException(String message) {
        super(message);
    }
}
