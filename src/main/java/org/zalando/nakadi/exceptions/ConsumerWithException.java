package org.zalando.nakadi.exceptions;


@FunctionalInterface
public interface ConsumerWithException<T> {

    void accept(T t) throws Exception;

}
