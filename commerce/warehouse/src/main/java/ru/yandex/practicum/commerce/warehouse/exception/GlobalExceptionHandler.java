package ru.yandex.practicum.commerce.warehouse.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import ru.yandex.practicum.commerce.dto.warehouse.exception.NoSpecifiedProductInWarehouseException;
import ru.yandex.practicum.commerce.dto.warehouse.exception.ProductInShoppingCartLowQuantityInWarehouse;
import ru.yandex.practicum.commerce.dto.warehouse.exception.SpecifiedProductAlreadyInWarehouseException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(SpecifiedProductAlreadyInWarehouseBusinessException.class)
    public ResponseEntity<SpecifiedProductAlreadyInWarehouseException> handleProductAlreadyInWarehouse(
            SpecifiedProductAlreadyInWarehouseBusinessException ex, WebRequest request) {

        log.warn("Product already in warehouse: {}", ex.getProductId());

        SpecifiedProductAlreadyInWarehouseException errorResponse = SpecifiedProductAlreadyInWarehouseException.builder()
                .message(ex.getMessage())
                .userMessage("Ошибка, товар с таким описанием уже зарегистрирован на складе")
                .httpStatus(HttpStatus.BAD_REQUEST)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTraceForProductAlready(ex.getStackTrace()))
                .cause(convertThrowableCauseForProductAlready(ex.getCause()))
                .suppressed(convertSuppressedForProductAlready(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(ProductInShoppingCartLowQuantityBusinessException.class)
    public ResponseEntity<ProductInShoppingCartLowQuantityInWarehouse> handleLowQuantity(
            ProductInShoppingCartLowQuantityBusinessException ex, WebRequest request) {

        log.warn("Insufficient products in warehouse: {}", ex.getInsufficientProducts());

        ProductInShoppingCartLowQuantityInWarehouse errorResponse = ProductInShoppingCartLowQuantityInWarehouse.builder()
                .message(ex.getMessage())
                .userMessage("Ошибка, товар из корзины не находится в требуемом количестве на складе")
                .httpStatus(HttpStatus.BAD_REQUEST)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTraceForLowQuantity(ex.getStackTrace()))
                .cause(convertThrowableCauseForLowQuantity(ex.getCause()))
                .suppressed(convertSuppressedForLowQuantity(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(NoSpecifiedProductInWarehouseBusinessException.class)
    public ResponseEntity<NoSpecifiedProductInWarehouseException> handleNoProductInWarehouse(
            NoSpecifiedProductInWarehouseBusinessException ex, WebRequest request) {

        log.warn("Product not found in warehouse: {}", ex.getProductId());

        NoSpecifiedProductInWarehouseException errorResponse = NoSpecifiedProductInWarehouseException.builder()
                .message(ex.getMessage())
                .userMessage("Нет информации о товаре на складе")
                .httpStatus(HttpStatus.BAD_REQUEST)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTraceForNoProduct(ex.getStackTrace()))
                .cause(convertThrowableCauseForNoProduct(ex.getCause()))
                .suppressed(convertSuppressedForNoProduct(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
    }


    private List<SpecifiedProductAlreadyInWarehouseException.StackTraceElement> convertStackTraceForProductAlready(java.lang.StackTraceElement[] stackTrace) {
        if (stackTrace == null) return Collections.emptyList();
        return Arrays.stream(stackTrace)
                .map(this::convertStackTraceElementForProductAlready)
                .collect(Collectors.toList());
    }

    private SpecifiedProductAlreadyInWarehouseException.StackTraceElement convertStackTraceElementForProductAlready(java.lang.StackTraceElement element) {
        return SpecifiedProductAlreadyInWarehouseException.StackTraceElement.builder()
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

    private List<SpecifiedProductAlreadyInWarehouseException.ThrowableCause> convertSuppressedForProductAlready(Throwable[] suppressed) {
        if (suppressed == null || suppressed.length == 0) return Collections.emptyList();
        return Arrays.stream(suppressed)
                .map(this::convertThrowableCauseForProductAlready)
                .collect(Collectors.toList());
    }

    private SpecifiedProductAlreadyInWarehouseException.ThrowableCause convertThrowableCauseForProductAlready(Throwable throwable) {
        if (throwable == null) return null;
        return SpecifiedProductAlreadyInWarehouseException.ThrowableCause.builder()
                .message(throwable.getMessage())
                .localizedMessage(throwable.getLocalizedMessage())
                .stackTrace(convertStackTraceForProductAlready(throwable.getStackTrace()))
                .build();
    }


    private List<ProductInShoppingCartLowQuantityInWarehouse.StackTraceElement> convertStackTraceForLowQuantity(java.lang.StackTraceElement[] stackTrace) {
        if (stackTrace == null) return Collections.emptyList();
        return Arrays.stream(stackTrace)
                .map(this::convertStackTraceElementForLowQuantity)
                .collect(Collectors.toList());
    }

    private ProductInShoppingCartLowQuantityInWarehouse.StackTraceElement convertStackTraceElementForLowQuantity(java.lang.StackTraceElement element) {
        return ProductInShoppingCartLowQuantityInWarehouse.StackTraceElement.builder()
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

    private List<ProductInShoppingCartLowQuantityInWarehouse.ThrowableCause> convertSuppressedForLowQuantity(Throwable[] suppressed) {
        if (suppressed == null || suppressed.length == 0) return Collections.emptyList();
        return Arrays.stream(suppressed)
                .map(this::convertThrowableCauseForLowQuantity)
                .collect(Collectors.toList());
    }

    private ProductInShoppingCartLowQuantityInWarehouse.ThrowableCause convertThrowableCauseForLowQuantity(Throwable throwable) {
        if (throwable == null) return null;
        return ProductInShoppingCartLowQuantityInWarehouse.ThrowableCause.builder()
                .message(throwable.getMessage())
                .localizedMessage(throwable.getLocalizedMessage())
                .stackTrace(convertStackTraceForLowQuantity(throwable.getStackTrace()))
                .build();
    }


    private List<NoSpecifiedProductInWarehouseException.StackTraceElement> convertStackTraceForNoProduct(java.lang.StackTraceElement[] stackTrace) {
        if (stackTrace == null) return Collections.emptyList();
        return Arrays.stream(stackTrace)
                .map(this::convertStackTraceElementForNoProduct)
                .collect(Collectors.toList());
    }

    private NoSpecifiedProductInWarehouseException.StackTraceElement convertStackTraceElementForNoProduct(java.lang.StackTraceElement element) {
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

    private List<NoSpecifiedProductInWarehouseException.ThrowableCause> convertSuppressedForNoProduct(Throwable[] suppressed) {
        if (suppressed == null || suppressed.length == 0) return Collections.emptyList();
        return Arrays.stream(suppressed)
                .map(this::convertThrowableCauseForNoProduct)
                .collect(Collectors.toList());
    }

    private NoSpecifiedProductInWarehouseException.ThrowableCause convertThrowableCauseForNoProduct(Throwable throwable) {
        if (throwable == null) return null;
        return NoSpecifiedProductInWarehouseException.ThrowableCause.builder()
                .message(throwable.getMessage())
                .localizedMessage(throwable.getLocalizedMessage())
                .stackTrace(convertStackTraceForNoProduct(throwable.getStackTrace()))
                .build();
    }


    @ExceptionHandler(Exception.class)
    public ResponseEntity<SpecifiedProductAlreadyInWarehouseException> handleGenericException(
            Exception ex, WebRequest request) {

        log.error("Internal server error: ", ex);

        SpecifiedProductAlreadyInWarehouseException errorResponse = SpecifiedProductAlreadyInWarehouseException.builder()
                .message("Internal server error")
                .userMessage("Произошла внутренняя ошибка сервера")
                .httpStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTraceForProductAlready(ex.getStackTrace()))
                .cause(convertThrowableCauseForProductAlready(ex.getCause()))
                .suppressed(convertSuppressedForProductAlready(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
