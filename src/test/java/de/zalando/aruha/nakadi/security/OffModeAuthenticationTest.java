package de.zalando.aruha.nakadi.security;


import de.zalando.aruha.nakadi.config.SecuritySettings;
import java.util.stream.Stream;
import org.junit.Test;

public class OffModeAuthenticationTest extends AuthenticationTest {

    static {
        authMode = SecuritySettings.AuthMode.OFF;
    }

    @Test
    public void offAuthMode() {
        Stream.concat(endpoints.stream(), endpointsForUidScope.stream())
                .forEach(this::checkHasCorrectResponseStatus);
    }

    public void checkHasCorrectResponseStatus(Endpoint endpoint) {
        try {
            mockMvc.perform(endpoint.toRequestBuilder()).andExpect(STATUS_NOT_401_OR_403);
        } catch (Exception e) {
            throw new AssertionError("Exception when calling endpoint: " + endpoint, e);
        } catch (AssertionError e) {
            throw new AssertionError("Assertion error when calling endpoint: " + endpoint, e);
        }

    }

}
