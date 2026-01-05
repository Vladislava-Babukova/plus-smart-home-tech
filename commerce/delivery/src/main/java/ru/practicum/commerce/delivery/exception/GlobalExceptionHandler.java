package ru.practicum.commerce.delivery.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import ru.yandex.practicum.commerce.dto.delivery.exception.NoDeliveryFoundException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(NoDeliveryFoundBusinessException.class)
    public ResponseEntity<NoDeliveryFoundException> handleNoDeliveryFoundException(
            NoDeliveryFoundBusinessException ex, WebRequest request) {

        log.warn("Delivery not found: {}", ex.getDeliveryId());

        NoDeliveryFoundException errorResponse = NoDeliveryFoundException.builder()
                .message(ex.getMessage())
                .userMessage("Доставка не найдена")
                .httpStatus(HttpStatus.NOT_FOUND)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTrace(ex.getStackTrace()))
                .cause(convertThrowableCause(ex.getCause()))
                .suppressed(convertSuppressed(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<NoDeliveryFoundException> handleGenericException(
            Exception ex, WebRequest request) {

        log.error("Internal server error: ", ex);

        NoDeliveryFoundException errorResponse = NoDeliveryFoundException.builder()
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

    private List<NoDeliveryFoundException.StackTraceElement> convertStackTrace(java.lang.StackTraceElement[] stackTrace) {
        if (stackTrace == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(stackTrace)
                .map(this::convertStackTraceElement)
                .collect(Collectors.toList());
    }

    private NoDeliveryFoundException.StackTraceElement convertStackTraceElement(java.lang.StackTraceElement element) {
        return NoDeliveryFoundException.StackTraceElement.builder()
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

    private List<NoDeliveryFoundException.ThrowableCause> convertSuppressed(Throwable[] suppressed) {
        if (suppressed == null || suppressed.length == 0) {
            return Collections.emptyList();
        }
        return Arrays.stream(suppressed)
                .map(this::convertThrowableCause)
                .collect(Collectors.toList());
    }

    private NoDeliveryFoundException.ThrowableCause convertThrowableCause(Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        return NoDeliveryFoundException.ThrowableCause.builder()
                .message(throwable.getMessage())
                .localizedMessage(throwable.getLocalizedMessage())
                .stackTrace(convertStackTrace(throwable.getStackTrace()))
                .build();
    }
}
