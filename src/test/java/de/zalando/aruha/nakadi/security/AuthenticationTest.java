package de.zalando.aruha.nakadi.security;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import de.zalando.aruha.nakadi.Application;
import de.zalando.aruha.nakadi.config.SecuritySettings;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeCache;
import de.zalando.aruha.nakadi.repository.db.EventTypeDbRepository;
import de.zalando.aruha.nakadi.repository.db.SubscriptionDbRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaTopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomUUID;
import java.util.List;
import java.util.Set;
import javax.annotation.PostConstruct;
import javax.servlet.Filter;
import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.SpringApplicationConfiguration;
import org.springframework.boot.test.WebIntegrationTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import static org.springframework.http.HttpMethod.DELETE;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.UNAUTHORIZED;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.OAuth2Request;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.springframework.test.annotation.DirtiesContext;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringApplicationConfiguration(Application.class)
@WebIntegrationTest(randomPort = true)
@DirtiesContext(classMode = AFTER_CLASS)
@ActiveProfiles("test")
public abstract class AuthenticationTest {

    @Configuration
    public static class Config {

        @Value("${nakadi.oauth2.scopes.uid}")
        protected String uidScope;

        @Value("${nakadi.oauth2.scopes.nakadiAdmin}")
        protected String nakadiAdminScope;

        @Value("${nakadi.oauth2.scopes.eventTypeWrite}")
        protected String eventTypeWriteScope;

        @Value("${nakadi.oauth2.scopes.eventStreamRead}")
        protected String eventStreamReadScope;

        @Value("${nakadi.oauth2.scopes.eventStreamWrite}")
        protected String eventStreamWriteScope;

        private final Multimap<String, String> scopesForTokens = ArrayListMultimap.create();

        @PostConstruct
        public void mockTokensScopes() {
            scopesForTokens.put(TOKEN_WITH_UID_SCOPE, uidScope);
            scopesForTokens.put(TOKEN_WITH_NAKADI_ADMIN_SCOPE, nakadiAdminScope);
            scopesForTokens.put(TOKEN_WITH_EVENT_TYPE_WRITE_SCOPE, eventTypeWriteScope);
            scopesForTokens.put(TOKEN_WITH_EVENT_STREAM_READ_SCOPE, eventStreamReadScope);
            scopesForTokens.put(TOKEN_WITH_EVENT_STREAM_WRITE_SCOPE, eventStreamWriteScope);
            scopesForTokens.put(TOKEN_WITH_RANDOM_SCOPE, randomUUID());
        }

        @Bean
        public ResourceServerTokenServices mockResourceTokenServices() {
            final ResourceServerTokenServices tokenServices = mock(ResourceServerTokenServices.class);

            when(tokenServices.loadAuthentication(any())).thenAnswer(invocation -> {

                final UsernamePasswordAuthenticationToken user = new UsernamePasswordAuthenticationToken("user", "N/A",
                        AuthorityUtils.commaSeparatedStringToAuthorityList("ROLE_USER"));

                final String token = (String) invocation.getArguments()[0];
                final Set<String> scopes = ImmutableSet.copyOf(scopesForTokens.get(token));

                final OAuth2Request request = new OAuth2Request(null, null, null, true, scopes, null, null, null, null);
                return new OAuth2Authentication(request, user);
            });
            return tokenServices;
        }

        @Bean
        public SecuritySettings mockSecuritySettings() {
            final SecuritySettings settings = mock(SecuritySettings.class);
            when(settings.getAuthMode()).thenReturn(authMode);
            return settings;
        }

        @Bean
        public EventTypeRepository mockDbRepository() {
            return mock(EventTypeDbRepository.class);
        }

        @Bean
        public SubscriptionDbRepository mockSubscriptionDbRepo() {
            return mock(SubscriptionDbRepository.class);
        }
        @Bean
        public EventTypeCache eventTypeCache() {
            return mock(EventTypeCache.class);
        }

        @Bean
        public ZooKeeperHolder mockZKHolder() {
            return mock(ZooKeeperHolder.class);
        }

        @Bean
        public KafkaTopicRepository mockkafkaRepository() {
            return mock(KafkaTopicRepository.class);
        }
    }

    protected static final ResultMatcher STATUS_NOT_401_OR_403 = status().is(not(
                isOneOf(UNAUTHORIZED.value(), FORBIDDEN.value())));

    protected static final String TOKEN_WITH_UID_SCOPE = randomUUID();
    protected static final String TOKEN_WITH_NAKADI_ADMIN_SCOPE = randomUUID();
    protected static final String TOKEN_WITH_EVENT_TYPE_WRITE_SCOPE = randomUUID();
    protected static final String TOKEN_WITH_EVENT_STREAM_READ_SCOPE = randomUUID();
    protected static final String TOKEN_WITH_EVENT_STREAM_WRITE_SCOPE = randomUUID();
    protected static final String TOKEN_WITH_RANDOM_SCOPE = randomUUID();

    protected static SecuritySettings.AuthMode authMode;

    protected static final List<Endpoint> endpoints = ImmutableList.of(
            new Endpoint(POST, "/event-types", TOKEN_WITH_EVENT_TYPE_WRITE_SCOPE),
            new Endpoint(PUT, "/event-types/foo", TOKEN_WITH_EVENT_TYPE_WRITE_SCOPE),
            new Endpoint(DELETE, "/event-types/foo", TOKEN_WITH_NAKADI_ADMIN_SCOPE),
            new Endpoint(POST, "/event-types/foo/events", TOKEN_WITH_EVENT_STREAM_WRITE_SCOPE),
            new Endpoint(GET, "/event-types/foo/events", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/event-types/foo/partitions", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/event-types/foo/partitions/bar", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/subscriptions/foo/events", TOKEN_WITH_EVENT_STREAM_READ_SCOPE));

    protected static final List<Endpoint> endpointsForUidScope = ImmutableList.of(
            new Endpoint(GET, "/metrics", TOKEN_WITH_UID_SCOPE),
            new Endpoint(GET, "/registry/partition-strategies", TOKEN_WITH_UID_SCOPE),
            new Endpoint(GET, "/event-types", TOKEN_WITH_UID_SCOPE),
            new Endpoint(GET, "/event-types/foo", TOKEN_WITH_UID_SCOPE),
            new Endpoint(GET, "/version", TOKEN_WITH_UID_SCOPE));

    private static final List<Endpoint> endpointsWithNoScope = ImmutableList.of(
            new Endpoint(GET, "/health"));


    @Autowired
    private WebApplicationContext applicationContext;

    @Autowired
    private Filter springSecurityFilterChain;

    protected MockMvc mockMvc;

    @Before
    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(applicationContext).addFilters(springSecurityFilterChain).build();
    }

    @Test
    public void testPublicEndpoints() {
        endpointsWithNoScope.forEach(this::checkOkResponse);
    }

    private void checkOkResponse(final Endpoint endpoint) {
        try {
            mockMvc.perform(endpoint.toRequestBuilder()).andExpect(status().isOk());
        } catch (final Exception e) {
            throw new AssertionError("Exception occurred while calling endpoint: " + endpoint, e);
        } catch (final AssertionError e) {
            throw new AssertionError("Assertion error on endpoint:" + endpoint, e);
        }
    }

}
