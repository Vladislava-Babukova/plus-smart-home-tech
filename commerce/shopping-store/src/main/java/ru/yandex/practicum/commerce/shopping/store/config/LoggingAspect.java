package ru.yandex.practicum.commerce.shopping.store.config;

import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;

@Slf4j
@Aspect
@Component
public class LoggingAspect {

    public LoggingAspect() {
        log.info("LoggingAspect initialized - AOP should work now!");
    }

    @Around("@annotation(ru.yandex.practicum.commerce.shopping.store.config.Loggable)")
    public Object logExecutionTime(ProceedingJoinPoint joinPoint) throws Throwable {
        log.debug("Entering method: {}", joinPoint.getSignature());
        Object[] args = joinPoint.getArgs();
        log.debug("Request Parameters: {}", args);

        Object result = joinPoint.proceed();

        log.debug("Exiting method: {} - Response: {}", joinPoint.getSignature(), result);
        return result;
    }
}