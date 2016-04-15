package de.zalando.aruha.nakadi.security;


import de.zalando.aruha.nakadi.config.SecuritySettings;
import org.junit.Test;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class FullModeAuthenticationTest extends AuthenticationTest {

    static {
        authMode = SecuritySettings.AuthMode.FULL;
    }

    @Test
    public void fullAuthMode() throws Exception {
        endpoints.forEach(this::checkHasOnlyAccessByNeededScope);
        endpointsForUidScope.forEach(this::checkHasAccessByUidScope);
    }

    private void checkHasAccessByUidScope(final Endpoint endpoint) {
        try {
            mockMvc.perform(endpoint.toRequestBuilder()).andExpect(STATUS_NOT_401_OR_403);
            mockMvc.perform(endpoint.withToken(null).toRequestBuilder()).andExpect(status().isUnauthorized());
        } catch (final Exception e) {
            throw new AssertionError("Error occurred when calling endpoint: " + endpoint, e);
        } catch (final AssertionError e) {
            throw new AssertionError("Assertion failed for endpoint: " + endpoint, e);
        }
    }

    private void checkHasOnlyAccessByNeededScope(final Endpoint endpoint) {
        try {
            // token with valid scope
            mockMvc.perform(endpoint.toRequestBuilder()).andExpect(STATUS_NOT_401_OR_403);

            // check that just a uid scope is not enough
            mockMvc.perform(endpoint.withToken(TOKEN_WITH_UID_SCOPE).toRequestBuilder())
                    .andExpect(status().isForbidden());

            // check random scope
            mockMvc.perform(endpoint.withToken(TOKEN_WITH_RANDOM_SCOPE).toRequestBuilder())
                    .andExpect(status().isForbidden());

            // no token at all
            mockMvc.perform(endpoint.withToken(null).toRequestBuilder())
                    .andExpect(status().isUnauthorized());
        } catch (final Exception e) {
            throw new AssertionError("Error occurred when calling endpoint: " + endpoint, e);
        } catch (final AssertionError e) {
            throw new AssertionError("Assertion failed for endpoint: " + endpoint, e);
        }
    }

}
