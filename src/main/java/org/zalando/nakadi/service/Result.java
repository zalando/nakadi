package org.zalando.nakadi.service;

import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.ws.rs.core.Response;
import java.util.function.Supplier;

public interface Result<T> {

    boolean isSuccessful();

    T getValue();

    Problem getProblem();

    static <T> Result<T> problem(final Problem problem) {
        return new Failure<>(problem);
    }

    static Result<Void> ok() {
        return new Success<>(null);
    }

    static <T> Result<T> ok(final T value) {
        return new Success<>(value);
    }

    static Result<Void> forbidden(final String message) {
        return problem(Problem.valueOf(Response.Status.FORBIDDEN, message));
    }

    static Result<Void> notFound(final String message) {
        return problem(Problem.valueOf(Response.Status.NOT_FOUND, message));
    }

    static ResponseEntity<?> wrap(final Supplier<Result> supplier, final NativeWebRequest request) {
        final Result result = supplier.get();
        if (!result.isSuccessful()) {
            return Responses.create(result.getProblem(), request);
        }
        return ResponseEntity.ok(result.getValue());
    }

    class Success<V> implements Result<V> {

        private final V value;

        public Success(final V value) {
            this.value = value;
        }

        @Override
        public boolean isSuccessful() {
            return true;
        }

        @Override
        public V getValue() {
            return value;
        }

        @Override
        public Problem getProblem() {
            throw new IllegalArgumentException("Success.getProblem");
        }
    }

    class Failure<T> implements Result<T> {

        private final Problem problem;

        public Failure(final Problem problem) {
            this.problem = problem;
        }

        @Override
        public boolean isSuccessful() {
            return false;
        }

        @Override
        public T getValue() {
            throw new IllegalArgumentException("Failure.getValue");
        }

        @Override
        public Problem getProblem() {
            return problem;
        }
    }
}
