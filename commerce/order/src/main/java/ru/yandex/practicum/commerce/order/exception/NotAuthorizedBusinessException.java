package ru.yandex.practicum.commerce.order.exception;

public class NotAuthorizedBusinessException extends RuntimeException {
    public NotAuthorizedBusinessException(String message) {
        super(message);
    }

    public NotAuthorizedBusinessException(String message, Throwable cause) {
        super(message, cause);
    }
}
