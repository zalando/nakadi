package org.zalando.nakadi.security;

import org.junit.Test;
import org.zalando.nakadi.config.SecuritySettings;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class RealmModeAuthenticationTest extends AuthenticationTest {

    static {
        authMode = SecuritySettings.AuthMode.REALM;
    }

    @Test
    public void realmAuthMode() throws Exception {
        ENDPOINTS_FOR_REALM.stream().forEach(this::checkHasOnlyAccessByRealm);
    }

    private void checkHasOnlyAccessByRealm(final Endpoint endpoint) {
        try {
            // token with correct realm
            mockMvc.perform(endpoint.withToken(TOKEN_WITH_REALM).toRequestBuilder())
                    .andExpect(STATUS_NOT_401_OR_403);

            // token with wrong realm
            mockMvc.perform(endpoint.withToken(TOKEN_WITH_WRONG_REALM).toRequestBuilder())
                    .andExpect(status().isForbidden())
                    .andExpect(content().string("{\"type\":\"https://httpstatus.es/403\",\"title\":\"Forbidden\"," +
                            "\"status\":403,\"detail\":\"Insufficient realms for this resource\"}"));

            // token with random scope
            mockMvc.perform(endpoint.withToken(TOKEN_WITH_RANDOM_SCOPE).toRequestBuilder())
                    .andExpect(status().isForbidden())
                    .andExpect(content().string("{\"type\":\"https://httpstatus.es/403\",\"title\":\"Forbidden\"," +
                            "\"status\":403,\"detail\":\"Insufficient realms for this resource\"}"));

            // token with uid scope
            mockMvc.perform(endpoint.withToken(TOKEN_WITH_UID_SCOPE).toRequestBuilder())
                    .andExpect(status().isForbidden())
                    .andExpect(content().string("{\"type\":\"https://httpstatus.es/403\",\"title\":\"Forbidden\"," +
                            "\"status\":403,\"detail\":\"Insufficient realms for this resource\"}"));

            // no token at all
            mockMvc.perform(endpoint.withToken(null).toRequestBuilder())
                    .andExpect(status().isUnauthorized())
                    .andExpect(content().string("{\"type\":\"https://httpstatus.es/401\",\"title\":\"Unauthorized\"," +
                            "\"status\":401,\"detail\":\"Full authentication is required to access this resource\"}"));
        } catch (final Exception e) {
            throw new AssertionError("Error occurred when calling endpoint: " + endpoint, e);
        } catch (AssertionError e) {
            throw new AssertionError("Assertion error on endpoint: " + endpoint, e);
        }
    }
}
