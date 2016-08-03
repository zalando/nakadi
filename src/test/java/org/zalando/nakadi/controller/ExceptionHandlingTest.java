package org.zalando.nakadi.controller;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.exceptions.IllegalClientIdException;
import org.zalando.nakadi.exceptions.IllegalScopeException;
import org.zalando.problem.Problem;

import java.util.Collections;

public class ExceptionHandlingTest {

    private static final String OAUTH2_SCOPE_WRITE = "oauth2.scope.write";

    @Test
    public void testHandleIllegalScopeException() {
        final ExceptionHandling exceptionHandling = new ExceptionHandling();
        final NativeWebRequest mockedRequest = Mockito.mock(NativeWebRequest.class);
        Mockito.when(mockedRequest.getHeader(Matchers.any())).thenReturn("");

        final ResponseEntity<Problem> problemResponseEntity = exceptionHandling.handleIllegalScopeException(
                new IllegalScopeException(Collections.singleton(OAUTH2_SCOPE_WRITE)), mockedRequest);

        Assert.assertEquals(problemResponseEntity.getStatusCode(), HttpStatus.FORBIDDEN);
        Assert.assertEquals(problemResponseEntity.getBody().getDetail().get(),
                "Client has to have scopes: [" + OAUTH2_SCOPE_WRITE + "]");
    }

    @Test
    public void testIllegalClientIdException() {
        final ExceptionHandling exceptionHandling = new ExceptionHandling();
        final NativeWebRequest mockedRequest = Mockito.mock(NativeWebRequest.class);
        Mockito.when(mockedRequest.getHeader(Matchers.any())).thenReturn("");

        final ResponseEntity<Problem> problemResponseEntity = exceptionHandling.handleIllegalClientIdException(
                new IllegalClientIdException("You don't have access to this event type"), mockedRequest);

        Assert.assertEquals(problemResponseEntity.getStatusCode(), HttpStatus.FORBIDDEN);
        Assert.assertEquals(problemResponseEntity.getBody().getDetail().get(),
                "You don't have access to this event type");
    }
}