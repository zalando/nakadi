package org.zalando.nakadi.exceptions;

@FunctionalInterface
public interface FunctionWithException<T, R> {
        R apply(T t) throws Exception;
}
