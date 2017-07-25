package org.zalando.nakadi.security;

import org.junit.Test;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.zalando.nakadi.config.SecuritySettings;

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
                    .andExpect(status().isForbidden());

            // token with random scope
            mockMvc.perform(endpoint.withToken(TOKEN_WITH_RANDOM_SCOPE).toRequestBuilder())
                    .andExpect(status().isForbidden());

            // token with uid scope
            mockMvc.perform(endpoint.withToken(TOKEN_WITH_UID_SCOPE).toRequestBuilder())
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
