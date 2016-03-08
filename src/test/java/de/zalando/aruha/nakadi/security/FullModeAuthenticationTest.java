package de.zalando.aruha.nakadi.security;


import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.config.SecuritySettings;
import org.junit.Test;

import java.util.List;

import static org.springframework.http.HttpMethod.DELETE;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class FullModeAuthenticationTest extends AuthenticationTest {

    static {
        authMode = SecuritySettings.AuthMode.FULL;
    }

    private static final List<Endpoint> endpoints = ImmutableList.of(
            new Endpoint(GET, "/event-types", TOKEN_WITH_NAKADI_READ_SCOPE),
            new Endpoint(POST, "/event-types", TOKEN_WITH_EVENT_TYPE_WRITE_SCOPE),
            new Endpoint(GET, "/event-types/foo", TOKEN_WITH_NAKADI_READ_SCOPE),
            new Endpoint(PUT, "/event-types/foo", TOKEN_WITH_EVENT_TYPE_WRITE_SCOPE),
            new Endpoint(DELETE, "/event-types/foo", TOKEN_WITH_NAKADI_ADMIN_SCOPE),
            new Endpoint(POST, "/event-types/foo/events", TOKEN_WITH_EVENT_STREAM_WRITE_SCOPE),
            new Endpoint(GET, "/event-types/foo/events", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/event-types/foo/partitions", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/event-types/foo/partitions/bar", TOKEN_WITH_EVENT_STREAM_READ_SCOPE));

    @Test
    public void fullAuthMode() throws Exception {
        endpoints.forEach(this::checkHasOnlyAccessByNeededScope);

        mockMvc.perform(addTokenHeader(get("/metrics"), TOKEN_WITH_UID_SCOPE)).andExpect(STATUS_NOT_401_OR_403);
        mockMvc.perform(get("/metrics")).andExpect(status().isUnauthorized());

        mockMvc.perform(get("/health")).andExpect(status().isOk());
    }

    private void checkHasOnlyAccessByNeededScope(final Endpoint endpoint) {
        try {
            // token with valid scope
            mockMvc.perform(addTokenHeader(endpoint.toRequestBuilder(), endpoint.getValidToken().get()))
                    .andExpect(STATUS_NOT_401_OR_403);

            // check that just a uid scope is not enough
            mockMvc.perform(addTokenHeader(endpoint.toRequestBuilder(), TOKEN_WITH_UID_SCOPE))
                    .andExpect(status().isForbidden());

            // check random scope
            mockMvc.perform(addTokenHeader(endpoint.toRequestBuilder(), TOKEN_WITH_RANDOM_SCOPE))
                    .andExpect(status().isForbidden());

            // no token at all
            mockMvc.perform(endpoint.toRequestBuilder())
                    .andExpect(status().isUnauthorized());
        }
        catch (Exception e) {
            throw new AssertionError("Error occurred when calling endpoint: " + endpoint, e);
        }
        catch (AssertionError e) {
            throw new AssertionError("Assertion failed for endpoint: " + endpoint, e);
        }
    }

}
