package org.zalando.nakadi.controller;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.controller.advice.NakadiProblemHandling;
import org.zalando.nakadi.exceptions.runtime.IllegalClientIdException;
import org.zalando.problem.Problem;

public class NakadiProblemHandlingTest {

    private static final String OAUTH2_SCOPE_WRITE = "oauth2.scope.write";

    @Test
    public void testIllegalClientIdException() {
        final NakadiProblemHandling nakadiProblemHandling = new NakadiProblemHandling();
        final NativeWebRequest mockedRequest = Mockito.mock(NativeWebRequest.class);
        Mockito.when(mockedRequest.getHeader(Matchers.any())).thenReturn("");

        final ResponseEntity<Problem> problemResponseEntity = nakadiProblemHandling.handleForbiddenResponses(
                new IllegalClientIdException("You don't have access to this event type"), mockedRequest);

        Assert.assertEquals(problemResponseEntity.getStatusCode(), HttpStatus.FORBIDDEN);
        Assert.assertEquals(problemResponseEntity.getBody().getDetail(),
                "You don't have access to this event type");
    }
}