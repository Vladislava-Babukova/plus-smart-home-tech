package ru.yandex.practicum.commerce.payment.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;
import ru.yandex.practicum.commerce.dto.order.exception.NoOrderFoundException;
import ru.yandex.practicum.commerce.dto.payment.exception.NotEnoughInfoInOrderToCalculateException;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(NoOrderFoundBusinessException.class)
    public ResponseEntity<NoOrderFoundException> handleNoPaymentFoundException(
            NoOrderFoundBusinessException ex, WebRequest request) {

        log.warn("Payment not found: {}", ex.getOrderId());

        NoOrderFoundException errorResponse = NoOrderFoundException.builder()
                .message(ex.getMessage())
                .userMessage("Заказ не найден")
                .httpStatus(HttpStatus.NOT_FOUND)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTraceForNoOrder(ex.getStackTrace()))
                .cause(convertThrowableCauseForNoOrder(ex.getCause()))
                .suppressed(convertSuppressedForNoOrder(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(NotEnoughInfoInOrderToCalculateBusinessException.class)
    public ResponseEntity<NotEnoughInfoInOrderToCalculateException> handleNotEnoughInfoException(
            NotEnoughInfoInOrderToCalculateBusinessException ex, WebRequest request) {

        log.warn("Not enough info in order to calculate: {}", ex.getMessage());

        NotEnoughInfoInOrderToCalculateException errorResponse = NotEnoughInfoInOrderToCalculateException.builder()
                .message(ex.getMessage())
                .userMessage("Недостаточно информации в заказе для расчёта")
                .httpStatus(HttpStatus.BAD_REQUEST)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTraceForNotEnoughInfo(ex.getStackTrace()))
                .cause(convertThrowableCauseForNotEnoughInfo(ex.getCause()))
                .suppressed(convertSuppressedForNotEnoughInfo(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.BAD_REQUEST);
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

    private List<NotEnoughInfoInOrderToCalculateException.StackTraceElement> convertStackTraceForNotEnoughInfo(java.lang.StackTraceElement[] stackTrace) {
        if (stackTrace == null) {
            return Collections.emptyList();
        }
        return Arrays.stream(stackTrace)
                .map(this::convertStackTraceElementForNotEnoughInfo)
                .collect(Collectors.toList());
    }

    private NotEnoughInfoInOrderToCalculateException.StackTraceElement convertStackTraceElementForNotEnoughInfo(java.lang.StackTraceElement element) {
        return NotEnoughInfoInOrderToCalculateException.StackTraceElement.builder()
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

    private List<NotEnoughInfoInOrderToCalculateException.ThrowableCause> convertSuppressedForNotEnoughInfo(Throwable[] suppressed) {
        if (suppressed == null || suppressed.length == 0) {
            return Collections.emptyList();
        }
        return Arrays.stream(suppressed)
                .map(this::convertThrowableCauseForNotEnoughInfo)
                .collect(Collectors.toList());
    }

    private NotEnoughInfoInOrderToCalculateException.ThrowableCause convertThrowableCauseForNotEnoughInfo(Throwable throwable) {
        if (throwable == null) {
            return null;
        }
        return NotEnoughInfoInOrderToCalculateException.ThrowableCause.builder()
                .message(throwable.getMessage())
                .localizedMessage(throwable.getLocalizedMessage())
                .stackTrace(convertStackTraceForNotEnoughInfo(throwable.getStackTrace()))
                .build();
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<NoOrderFoundException> handleGenericException(
            Exception ex, WebRequest request) {

        log.error("Internal server error: ", ex);

        NoOrderFoundException errorResponse = NoOrderFoundException.builder()
                .message("Internal server error")
                .userMessage("Произошла внутренняя ошибка сервера")
                .httpStatus(HttpStatus.INTERNAL_SERVER_ERROR)
                .localizedMessage(ex.getLocalizedMessage())
                .stackTrace(convertStackTraceForNoOrder(ex.getStackTrace()))
                .cause(convertThrowableCauseForNoOrder(ex.getCause()))
                .suppressed(convertSuppressedForNoOrder(ex.getSuppressed()))
                .build();

        return new ResponseEntity<>(errorResponse, HttpStatus.INTERNAL_SERVER_ERROR);
    }
}
