package org.zalando.nakadi.security;


import org.junit.Test;
import org.zalando.nakadi.config.SecuritySettings;

import java.util.stream.Stream;

public class OffModeAuthenticationTest extends AuthenticationTest {

    static {
        authMode = SecuritySettings.AuthMode.OFF;
    }

    @Test
    public void offAuthMode() {
        Stream.concat(ENDPOINTS.stream(), ENDPOINTS_FOR_UID_SCOPE.stream())
                .forEach(this::checkHasCorrectResponseStatus);
    }

    public void checkHasCorrectResponseStatus(final Endpoint endpoint) {
        try {
            mockMvc.perform(endpoint.toRequestBuilder()).andExpect(STATUS_NOT_401_OR_403);
        } catch (Exception e) {
            throw new AssertionError("Exception when calling endpoint: " + endpoint, e);
        } catch (AssertionError e) {
            throw new AssertionError("Assertion error when calling endpoint: " + endpoint, e);
        }

    }

}
