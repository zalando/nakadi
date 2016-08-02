package org.zalando.nakadi.controller;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.exceptions.IllegalScopeException;
import org.zalando.problem.Problem;

import java.util.Collections;

public class ExceptionHandlingTest {

    private static final String OAUTH2_SCOPE_WRITE = "oauth2.scope.write";

    @Test
    public void testHandleIllegalScopeException() {
        ExceptionHandling exceptionHandling = new ExceptionHandling();
        NativeWebRequest mockedRequest = Mockito.mock(NativeWebRequest.class);
        Mockito.when(mockedRequest.getHeader(Matchers.any())).thenReturn("");

        ResponseEntity<Problem> problemResponseEntity = exceptionHandling.handleIllegalScopeException(
                new IllegalScopeException(Collections.singleton(OAUTH2_SCOPE_WRITE)), mockedRequest);

        Assert.assertEquals(problemResponseEntity.getStatusCode(), HttpStatus.FORBIDDEN);
        Assert.assertEquals(problemResponseEntity.getBody().getDetail().get(),
                "Client has to have scopes: [" + OAUTH2_SCOPE_WRITE + "]");
    }
}