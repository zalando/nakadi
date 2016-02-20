package de.zalando.aruha.nakadi.security;


import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.Application;
import de.zalando.aruha.nakadi.config.SecuritySettings;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.List;

import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(Application.class)
@WebIntegrationTest
@DirtiesContext(classMode = AFTER_CLASS)
public class OffModeAuthenticationTest extends AuthenticationTest {

    static {
        authMode = SecuritySettings.AuthMode.OFF;
    }

    private static final List<Endpoint> endpoints = ImmutableList.of(
            new Endpoint(GET, "/event-types"),
            new Endpoint(POST, "/event-types"),
            new Endpoint(GET, "/event-types/foo"),
            new Endpoint(PUT, "/event-types/foo"),
            new Endpoint(POST, "/event-types/foo/events"),
            new Endpoint(GET, "/event-types/foo/events"),
            new Endpoint(GET, "/event-types/foo/partitions"),
            new Endpoint(GET, "/event-types/foo/partitions/bar"),
            new Endpoint(GET, "/health"),
            new Endpoint(GET, "/metrics"));

    @Test
    public void offAuthMode() throws Exception {
        for (final Endpoint endpoint : endpoints) {
            mockMvc.perform(endpoint.toRequestBuilder()).andExpect(STATUS_NOT_401_OR_403);
        }
    }

}
