package ru.yandex.practicum.commerce.shopping.store.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import ru.yandex.practicum.commerce.dto.shopping.store.exception.ProductNotFoundException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ProductNotFoundBusinessException.class)
    public ResponseEntity<ProductNotFoundException> handleProductNotFoundException(
            ProductNotFoundBusinessException ex, WebRequest request) {

        log.warn("Product not found: {}", ex.getMessage());

        ProductNotFoundException errorResponse = ProductNotFoundException.builder()
                .message(ex.getMessage())
                .userMessage("Товар не найден в каталоге")
                .httpStatus(HttpStatus.NOT_FOUND)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTrace(ex.getStackTrace()))
                .cause(convertThrowableCause(ex.getCause()))
                .suppressed(convertSuppressed(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ProductNotFoundException> handleGenericException(
            Exception ex, WebRequest request) {

        log.error("Internal server error: ", ex);

        ProductNotFoundException errorResponse = ProductNotFoundException.builder()
                .message("Internal server error")
                .userMessage("Произошла внутренняя ошибка сервера")
                .httpStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTrace(ex.getStackTrace()))
                .cause(convertThrowableCause(ex.getCause()))
                .suppressed(convertSuppressed(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
    }


    private List<ProductNotFoundException.StackTraceElement> convertStackTrace(java.lang.StackTraceElement[] stackTrace) {
        if (stackTrace == null) {
            return Collections.emptyList();
        }

        return Arrays.stream(stackTrace)
                .map(this::convertStackTraceElement)
                .collect(Collectors.toList());
    }

    private ProductNotFoundException.StackTraceElement convertStackTraceElement(java.lang.StackTraceElement element) {
        return ProductNotFoundException.StackTraceElement.builder()
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


    private List<ProductNotFoundException.ThrowableCause> convertSuppressed(Throwable[] suppressed) {
        if (suppressed == null || suppressed.length == 0) {
            return Collections.emptyList();
        }

        return Arrays.stream(suppressed)
                .map(this::convertThrowableCause)
                .collect(Collectors.toList());
    }


    private ProductNotFoundException.ThrowableCause convertThrowableCause(Throwable throwable) {
        if (throwable == null) {
            return null;
        }

        return ProductNotFoundException.ThrowableCause.builder()
                .message(throwable.getMessage())
                .localizedMessage(throwable.getLocalizedMessage())
                .stackTrace(convertStackTrace(throwable.getStackTrace()))
                .build();
    }
}
