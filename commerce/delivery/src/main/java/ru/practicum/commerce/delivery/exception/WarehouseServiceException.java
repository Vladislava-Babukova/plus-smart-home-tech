package ru.practicum.commerce.delivery.exception;

public class WarehouseServiceException extends RuntimeException {
    public WarehouseServiceException(String message, Exception e) {
        super(message);
    }
}
