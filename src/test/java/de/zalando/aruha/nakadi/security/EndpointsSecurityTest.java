package de.zalando.aruha.nakadi.security;


import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import de.zalando.aruha.nakadi.Application;
import de.zalando.aruha.nakadi.config.SecuritySettings;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeDbRepository;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.OAuth2Request;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.annotation.PostConstruct;
import javax.servlet.Filter;
import java.util.List;
import java.util.Set;

import static de.zalando.aruha.nakadi.utils.TestUtils.randomString;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.UNAUTHORIZED;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(Application.class)
@WebIntegrationTest
public class EndpointsSecurityTest {

    @Configuration
    static class Config {
        @Bean
        @Primary
        public ResourceServerTokenServices fakeResourceTokenServices() {
            final ResourceServerTokenServices tokenServices = mock(ResourceServerTokenServices.class);

            when(tokenServices.loadAuthentication(any())).thenAnswer(invocation -> {

                UsernamePasswordAuthenticationToken user = new UsernamePasswordAuthenticationToken("user", "N/A",
                        AuthorityUtils.commaSeparatedStringToAuthorityList("ROLE_USER"));

                final String token = (String) invocation.getArguments()[0];
                final Set<String> scopes = ImmutableSet.copyOf(scopesForTokens.get(token));

                OAuth2Request request = new OAuth2Request(null, null, null, true, scopes, null, null, null, null);
                return new OAuth2Authentication(request, user);
            });
            return tokenServices;
        }

        @Bean
        @Primary
        public SecuritySettings securitySettings() {
            final SecuritySettings settings = mock(SecuritySettings.class);
            when(settings.getAuthMode()).thenReturn(SecuritySettings.AuthMode.BASIC);
            return settings;
        }

        @Bean
        @Primary
        public TopicRepository topicRepository() {
            return mock(TopicRepository.class);
        }

        @Bean
        @Primary
        public EventTypeDbRepository dbRepository() {
            return mock(EventTypeDbRepository.class);
        }
    }

    public static final ResultMatcher STATUS_NOT_401_OR_403 = status()
            .is(not(isOneOf(UNAUTHORIZED.value(), FORBIDDEN.value())));

    private static final String TOKEN_WITH_UID_SCOPE = randomString();
    private static final String TOKEN_WITH_RANDOM_SCOPE = randomString();

    private static final Multimap<String, String> scopesForTokens = ArrayListMultimap.create();

    @Value("${nakadi.oauth2.scopes.uid}")
    private String uidScope;

    @Value("${nakadi.oauth2.scopes.nakadiRead}")
    private String nakadiReadScope;

    @Value("${nakadi.oauth2.scopes.eventTypeWrite}")
    private String eventTypeWriteScope;

    @Value("${nakadi.oauth2.scopes.eventStreamRead}")
    private String eventStreamReadScope;

    @Value("${nakadi.oauth2.scopes.eventStreamWrite}")
    private String eventStreamWriteScope;

    @Autowired
    private WebApplicationContext applicationContext;

    @Autowired
    private Filter springSecurityFilterChain;

    private MockMvc mockMvc;

    private static final List<Endpoint> endpoints = ImmutableList.of(
            new Endpoint(GET, "/event-types"),
            new Endpoint(POST, "/event-types"),
            new Endpoint(GET, "/event-types/foo"),
            new Endpoint(PUT, "/event-types/foo"),
            new Endpoint(POST, "/event-types/foo/events"),
            new Endpoint(GET, "/event-types/foo/events"),
            new Endpoint(GET, "/event-types/foo/partitions"),
            new Endpoint(GET, "/event-types/foo/partitions/bar"),
            new Endpoint(GET, "/metrics"));

    @PostConstruct
    public void fillTokenMockData() {
        scopesForTokens.put(TOKEN_WITH_UID_SCOPE, uidScope);
        scopesForTokens.put(TOKEN_WITH_RANDOM_SCOPE, randomString());
    }

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders
                .webAppContextSetup(applicationContext)
                .addFilters(springSecurityFilterChain)
                .build();
    }

    @Test
    public void basicAuthMode() throws Exception {
        endpoints.forEach(this::checkUidEndpoint);
        mockMvc.perform(get("/health")).andExpect(status().isOk());
    }

    private void checkUidEndpoint(final Endpoint endpoint) {
        try {
            mockMvc.perform(addTokenHeader(endpoint.toRequestBuilder(), TOKEN_WITH_UID_SCOPE))
                    .andExpect(STATUS_NOT_401_OR_403);

            mockMvc.perform(addTokenHeader(endpoint.toRequestBuilder(), TOKEN_WITH_RANDOM_SCOPE))
                    .andExpect(status().isForbidden());

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

    private MockHttpServletRequestBuilder addTokenHeader(final MockHttpServletRequestBuilder builder,
                                                         final String token) {
        return builder.header("Authorization", "Bearer " + token);
    }

}
