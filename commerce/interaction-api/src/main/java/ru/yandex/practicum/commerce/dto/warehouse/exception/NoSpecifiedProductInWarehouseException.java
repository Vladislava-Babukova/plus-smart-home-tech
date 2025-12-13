package ru.yandex.practicum.commerce.dto.warehouse.exception;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.http.HttpStatus;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NoSpecifiedProductInWarehouseException {
    private ThrowableCause cause;
    private List<StackTraceElement> stackTrace;
    private HttpStatus httpStatus;
    private String userMessage;
    private String message;
    private List<ThrowableCause> suppressed;
    private String localizedMessage;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class ThrowableCause {
        private List<StackTraceElement> stackTrace;
        private String message;
        private String localizedMessage;
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class StackTraceElement {
        private String classLoaderName;
        private String moduleName;
        private String moduleVersion;
        private String methodName;
        private String fileName;
        private Integer lineNumber;
        private String className;
        private Boolean nativeMethod;
    }
}
