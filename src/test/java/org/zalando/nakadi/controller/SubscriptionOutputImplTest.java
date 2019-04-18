package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.mock.web.MockHttpServletResponse;
import org.zalando.nakadi.exceptions.runtime.ConflictException;

import java.io.IOException;
import java.io.OutputStream;

public class SubscriptionOutputImplTest {

    @Test
    public void testProblemRaisedForConflictException() {
        final SubscriptionStreamController ssc =
                new SubscriptionStreamController(null, new ObjectMapper(), null, null, null, null, null, null);

        final SubscriptionStreamController.SubscriptionOutputImpl impl =
                ssc.new SubscriptionOutputImpl(
                        new MockHttpServletResponse() {
                            @Override
                            public void setStatus(final int sc) {
                                Assert.assertEquals(HttpStatus.CONFLICT.value(), sc);
                            }
                        },
                        new OutputStream() {
                            @Override
                            public void write(final int b) throws IOException {
                                // skip
                            }
                        });

        impl.onException(new ConflictException("conflict during reset"));
    }
}
