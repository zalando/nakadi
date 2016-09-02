package org.zalando.nakadi.service;

import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.problem.spring.web.advice.Responses;

import static org.springframework.http.HttpStatus.OK;
import static org.springframework.http.ResponseEntity.status;

public class WrappableResult {

    public interface Wrappable {
        Result process();
    }

    public static ResponseEntity<?> resultWrapper(final Wrappable wrappable, final NativeWebRequest request) {
        final Result result = wrappable.process();
        if (!result.isSuccessful()) {
            return Responses.create(result.getProblem(), request);
        }
        return status(OK).body(result.getValue());
    }
}
