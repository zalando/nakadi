package org.zalando.nakadi.service;

import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.problem.spring.web.advice.Responses;

import java.util.function.Supplier;

import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.ResponseEntity.status;

public class WebResult {

    public static ResponseEntity<?> wrap(final Supplier<Result> supplier, final NativeWebRequest request) {
        final Result result = supplier.get();
        if (!result.isSuccessful()) {
            return Responses.create(result.getProblem(), request);
        }
        return status(OK).body(result.getValue());
    }
}
