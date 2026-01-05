package ru.yandex.practicum.commerce.payment.exception;

public class NotEnoughInfoInOrderToCalculateBusinessException extends RuntimeException {
    public NotEnoughInfoInOrderToCalculateBusinessException(String message) {
        super(message);
    }

    public NotEnoughInfoInOrderToCalculateBusinessException(String message, Throwable cause) {
        super(message, cause);
    }
}
