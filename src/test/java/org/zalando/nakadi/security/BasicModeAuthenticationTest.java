package org.zalando.nakadi.security;


import org.junit.Test;
import org.zalando.nakadi.config.SecuritySettings;

import java.util.stream.Stream;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class BasicModeAuthenticationTest extends AuthenticationTest {

    static {
        authMode = SecuritySettings.AuthMode.BASIC;
    }

    @Test
    public void basicAuthMode() throws Exception {
        Stream.concat(ENDPOINTS.stream(), ENDPOINTS_FOR_UID_SCOPE.stream()).forEach(this::checkHasOnlyAccessByUidScope);
    }

    private void checkHasOnlyAccessByUidScope(final Endpoint endpoint) {
        try {
            // basic uid scope
            mockMvc.perform(endpoint.withToken(TOKEN_WITH_UID_SCOPE).toRequestBuilder())
                    .andExpect(STATUS_NOT_401_OR_403);

            // token with random scope
            mockMvc.perform(endpoint.withToken(TOKEN_WITH_RANDOM_SCOPE).toRequestBuilder())
                    .andExpect(status().isForbidden());

            // no token at all
            mockMvc.perform(endpoint.withToken(null).toRequestBuilder())
                    .andExpect(status().isUnauthorized());
        } catch (final Exception e) {
            throw new AssertionError("Error occurred when calling endpoint: " + endpoint, e);
        } catch (AssertionError e) {
            throw new AssertionError("Assertion error on endpoint: " + endpoint, e);
        }
    }

}
