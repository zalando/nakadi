package de.zalando.aruha.nakadi.exceptions;

@FunctionalInterface
public interface FunctionWithException<T, R> {
        R apply(T t) throws Exception;
}
