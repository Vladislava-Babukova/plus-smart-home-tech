package ru.yandex.practicum.commerce.shopping.cart.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import ru.yandex.practicum.commerce.dto.shopping.cart.exception.NoProductsInShoppingCartException;
import ru.yandex.practicum.commerce.dto.shopping.cart.exception.NotAuthorizedUserException;

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

    @ExceptionHandler(NoProductsInShoppingCartBusinessException.class)
    public ResponseEntity<NoProductsInShoppingCartException> handleNoProductsException(
            NoProductsInShoppingCartBusinessException ex, WebRequest request) {

        log.warn("Products not found in cart: {}", ex.getProductIds());

        NoProductsInShoppingCartException errorResponse = NoProductsInShoppingCartException.builder()
                .message(ex.getMessage())
                .userMessage("Нет искомых товаров в корзине")
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


    private List<NoProductsInShoppingCartException.StackTraceElement> convertStackTraceForNoProducts(java.lang.StackTraceElement[] stackTrace) {
        if (stackTrace == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(stackTrace)
                .map(this::convertStackTraceElementForNoProducts)
                .collect(Collectors.toList());
    }

    private NoProductsInShoppingCartException.StackTraceElement convertStackTraceElementForNoProducts(java.lang.StackTraceElement element) {
        return NoProductsInShoppingCartException.StackTraceElement.builder()
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

    private List<NoProductsInShoppingCartException.ThrowableCause> convertSuppressedForNoProducts(Throwable[] suppressed) {
        if (suppressed == null || suppressed.length == 0) {
            return Collections.emptyList();
        }
        return Arrays.stream(suppressed)
                .map(this::convertThrowableCauseForNoProducts)
                .collect(Collectors.toList());
    }

    private NoProductsInShoppingCartException.ThrowableCause convertThrowableCauseForNoProducts(Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        return NoProductsInShoppingCartException.ThrowableCause.builder()
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