package org.zalando.nakadi.exceptions;

import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;

import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Function;

public abstract class Try<T> {

    private Try() { }

    public static <T> Try<T> cons(final Callable<T> func) {
        try {
            return Try.success(func.call());
        } catch (Exception e) {
            return Try.failure(e);
        }
    }

    public static <A, T> Function<A, Try<T>> wrap(final FunctionWithException<A, T> function) {
        return arg -> Try.cons(() -> function.apply(arg));
    }

    public static <T> Try<T> success(final T value) {
        return new Success<>(value);
    }

    public static <T> Try<T> failure(final Exception exception) {
        return new Failure<>(exception);
    }

    public abstract boolean isFailure();

    public abstract boolean isSuccess();

    public abstract T getOrThrow();

    public abstract T get();

    public abstract Optional<T> getO();

    public static final class Success<T> extends Try<T> {

        private final T value;

        private Success(final T value) {
            this.value = value;
        }

        @Override
        public boolean isFailure() {
            return false;
        }

        @Override
        public boolean isSuccess() {
            return true;
        }

        @Override
        public T getOrThrow() {
            return value;
        }

        @Override
        public T get() {
            return value;
        }

        @Override
        public Optional<T> getO() {
            return Optional.of(value);
        }

    }

    public static final class Failure<T> extends Try<T> {

        private final Exception exception;

        private Failure(final Exception exception) {
            this.exception = exception;
        }

        @Override
        public boolean isFailure() {
            return true;
        }

        @Override
        public boolean isSuccess() {
            return false;
        }

        @Override
        public T getOrThrow() {
            if (exception instanceof NakadiRuntimeException) {
                throw (NakadiRuntimeException) exception;
            } else if (exception instanceof NakadiBaseException) {
                throw (NakadiBaseException) exception;
            }
            throw new NakadiRuntimeException(exception);
        }

        @Override
        public T get() {
            throw new UnsupportedOperationException("Failure.get");
        }

        @Override
        public Optional<T> getO() {
            return Optional.empty();
        }

    }

    @FunctionalInterface
    public interface FunctionWithException<T, R> {

        R apply(T t) throws Exception;
    }
}