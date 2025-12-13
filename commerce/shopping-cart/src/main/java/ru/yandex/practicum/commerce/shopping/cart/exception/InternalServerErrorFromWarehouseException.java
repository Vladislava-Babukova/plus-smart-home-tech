package ru.yandex.practicum.commerce.shopping.cart.exception;

public class InternalServerErrorFromWarehouseException extends RuntimeException {
    public InternalServerErrorFromWarehouseException(String message) {
        super(message);
    }
}