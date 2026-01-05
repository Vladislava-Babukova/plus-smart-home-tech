package ru.yandex.practicum.commerce.order.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import ru.yandex.practicum.commerce.dto.order.exception.NoOrderFoundException;
import ru.yandex.practicum.commerce.dto.shopping.cart.exception.NotAuthorizedUserException;
import ru.yandex.practicum.commerce.dto.warehouse.exception.NoSpecifiedProductInWarehouseException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(NotAuthorizedBusinessException.class)
    public ResponseEntity<NotAuthorizedUserException> handleNotAuthorizedException(
            NotAuthorizedBusinessException ex, WebRequest request) {

        log.warn("Not authorized: {}", ex.getMessage());

        NotAuthorizedUserException errorResponse = NotAuthorizedUserException.builder()
                .message(ex.getMessage())
                .userMessage("Имя пользователя не должно быть пустым")
                .httpStatus(HttpStatus.UNAUTHORIZED)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTraceForNotAuthorized(ex.getStackTrace()))
                .cause(convertThrowableCauseForNotAuthorized(ex.getCause()))
                .suppressed(convertSuppressedForNotAuthorized(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.UNAUTHORIZED);
    }

    @ExceptionHandler(NoOrderFoundBusinessException.class)
    public ResponseEntity<NoOrderFoundException> handleNoOrderFoundException(
            NoOrderFoundBusinessException ex, WebRequest request) {

        log.warn("Order not found: {}", ex.getOrderId());

        NoOrderFoundException errorResponse = NoOrderFoundException.builder()
                .message(ex.getMessage())
                .userMessage("Не найден заказ")
                .httpStatus(HttpStatus.BAD_REQUEST)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTraceForNoOrder(ex.getStackTrace()))
                .cause(convertThrowableCauseForNoOrder(ex.getCause()))
                .suppressed(convertSuppressedForNoOrder(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(NoSpecifiedProductInWarehouseBusinessException.class)
    public ResponseEntity<NoSpecifiedProductInWarehouseException> handleNoProductsInWarehouseException(
            NoSpecifiedProductInWarehouseBusinessException ex, WebRequest request) {

        log.warn("Products not available in warehouse: {}", ex.getProductIds());

        NoSpecifiedProductInWarehouseException errorResponse = NoSpecifiedProductInWarehouseException.builder()
                .message(ex.getMessage())
                .userMessage("Нет заказываемого товара на складе")
                .httpStatus(HttpStatus.BAD_REQUEST)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTraceForNoProducts(ex.getStackTrace()))
                .cause(convertThrowableCauseForNoProducts(ex.getCause()))
                .suppressed(convertSuppressedForNoProducts(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
    }

    private List<NotAuthorizedUserException.StackTraceElement> convertStackTraceForNotAuthorized(java.lang.StackTraceElement[] stackTrace) {
        if (stackTrace == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(stackTrace)
                .map(this::convertStackTraceElementForNotAuthorized)
                .collect(Collectors.toList());
    }

    private NotAuthorizedUserException.StackTraceElement convertStackTraceElementForNotAuthorized(java.lang.StackTraceElement element) {
        return NotAuthorizedUserException.StackTraceElement.builder()
                .classLoaderName(element.getClassLoaderName())
                .moduleName(element.getModuleName())
                .moduleVersion(element.getModuleVersion())
                .methodName(element.getMethodName())
                .fileName(element.getFileName())
                .lineNumber(element.getLineNumber())
                .className(element.getClassName())
                .nativeMethod(element.isNativeMethod())
                .build();
    }

    private List<NotAuthorizedUserException.ThrowableCause> convertSuppressedForNotAuthorized(Throwable[] suppressed) {
        if (suppressed == null || suppressed.length == 0) {
            return Collections.emptyList();
        }
        return Arrays.stream(suppressed)
                .map(this::convertThrowableCauseForNotAuthorized)
                .collect(Collectors.toList());
    }

    private NotAuthorizedUserException.ThrowableCause convertThrowableCauseForNotAuthorized(Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        return NotAuthorizedUserException.ThrowableCause.builder()
                .message(throwable.getMessage())
                .localizedMessage(throwable.getLocalizedMessage())
                .stackTrace(convertStackTraceForNotAuthorized(throwable.getStackTrace()))
                .build();
    }

    private List<NoOrderFoundException.StackTraceElement> convertStackTraceForNoOrder(java.lang.StackTraceElement[] stackTrace) {
        if (stackTrace == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(stackTrace)
                .map(this::convertStackTraceElementForNoOrder)
                .collect(Collectors.toList());
    }

    private NoOrderFoundException.StackTraceElement convertStackTraceElementForNoOrder(java.lang.StackTraceElement element) {
        return NoOrderFoundException.StackTraceElement.builder()
                .classLoaderName(element.getClassLoaderName())
                .moduleName(element.getModuleName())
                .moduleVersion(element.getModuleVersion())
                .methodName(element.getMethodName())
                .fileName(element.getFileName())
                .lineNumber(element.getLineNumber())
                .className(element.getClassName())
                .nativeMethod(element.isNativeMethod())
                .build();
    }

    private List<NoOrderFoundException.ThrowableCause> convertSuppressedForNoOrder(Throwable[] suppressed) {
        if (suppressed == null || suppressed.length == 0) {
            return Collections.emptyList();
        }
        return Arrays.stream(suppressed)
                .map(this::convertThrowableCauseForNoOrder)
                .collect(Collectors.toList());
    }

    private NoOrderFoundException.ThrowableCause convertThrowableCauseForNoOrder(Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        return NoOrderFoundException.ThrowableCause.builder()
                .message(throwable.getMessage())
                .localizedMessage(throwable.getLocalizedMessage())
                .stackTrace(convertStackTraceForNoOrder(throwable.getStackTrace()))
                .build();
    }

    private List<NoSpecifiedProductInWarehouseException.StackTraceElement> convertStackTraceForNoProducts(java.lang.StackTraceElement[] stackTrace) {
        if (stackTrace == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(stackTrace)
                .map(this::convertStackTraceElementForNoProducts)
                .collect(Collectors.toList());
    }

    private NoSpecifiedProductInWarehouseException.StackTraceElement convertStackTraceElementForNoProducts(java.lang.StackTraceElement element) {
        return NoSpecifiedProductInWarehouseException.StackTraceElement.builder()
                .classLoaderName(element.getClassLoaderName())
                .moduleName(element.getModuleName())
                .moduleVersion(element.getModuleVersion())
                .methodName(element.getMethodName())
                .fileName(element.getFileName())
                .lineNumber(element.getLineNumber())
                .className(element.getClassName())
                .nativeMethod(element.isNativeMethod())
                .build();
    }

    private List<NoSpecifiedProductInWarehouseException.ThrowableCause> convertSuppressedForNoProducts(Throwable[] suppressed) {
        if (suppressed == null || suppressed.length == 0) {
            return Collections.emptyList();
        }
        return Arrays.stream(suppressed)
                .map(this::convertThrowableCauseForNoProducts)
                .collect(Collectors.toList());
    }

    private NoSpecifiedProductInWarehouseException.ThrowableCause convertThrowableCauseForNoProducts(Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        return NoSpecifiedProductInWarehouseException.ThrowableCause.builder()
                .message(throwable.getMessage())
                .localizedMessage(throwable.getLocalizedMessage())
                .stackTrace(convertStackTraceForNoProducts(throwable.getStackTrace()))
                .build();
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<NotAuthorizedUserException> handleGenericException(
            Exception ex, WebRequest request) {

        log.error("Internal server error: ", ex);

        NotAuthorizedUserException errorResponse = NotAuthorizedUserException.builder()
                .message("Internal server error")
                .userMessage("Произошла внутренняя ошибка сервера")
                .httpStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTraceForNotAuthorized(ex.getStackTrace()))
                .cause(convertThrowableCauseForNotAuthorized(ex.getCause()))
                .suppressed(convertSuppressedForNotAuthorized(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
