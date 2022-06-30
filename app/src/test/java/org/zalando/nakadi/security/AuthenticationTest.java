package org.zalando.nakadi.security;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.authority.AuthorityUtils;
import org.springframework.security.oauth2.provider.OAuth2Authentication;
import org.springframework.security.oauth2.provider.OAuth2Request;
import org.springframework.security.oauth2.provider.token.ResourceServerTokenServices;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.zalando.nakadi.Application;
import org.zalando.nakadi.cache.ChangesRegistry;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.metrics.EventTypeMetricRegistry;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.KafkaLocationManager;
import org.zalando.nakadi.repository.kafka.PartitionsCalculator;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.CursorsService;
import org.zalando.nakadi.service.EventStreamFactory;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.publishing.EventPublisher;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.stups.oauth2.spring.security.expression.ExtendedOAuth2WebSecurityExpressionHandler;

import javax.annotation.PostConstruct;
import javax.servlet.Filter;
import javax.sql.DataSource;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.hamcrest.Matchers.isOneOf;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.HttpMethod.DELETE;
import static org.springframework.http.HttpMethod.GET;
import static org.springframework.http.HttpMethod.POST;
import static org.springframework.http.HttpMethod.PUT;
import static org.springframework.http.HttpStatus.FORBIDDEN;
import static org.springframework.http.HttpStatus.UNAUTHORIZED;
import static org.springframework.test.annotation.DirtiesContext.ClassMode.AFTER_CLASS;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringJUnit4ClassRunner.class)
@SpringBootTest(classes = Application.class, webEnvironment = WebEnvironment.RANDOM_PORT, value = {"management.port=0"})
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

        @Autowired
        private Environment environment;

        private final Multimap<String, String> scopesForTokens = ArrayListMultimap.create();

        private final Map<String, String> realms = new HashMap<>();

        @PostConstruct
        public void mockTokensScopes() {
            scopesForTokens.put(TOKEN_WITH_UID_SCOPE, uidScope);
            scopesForTokens.put(TOKEN_WITH_NAKADI_ADMIN_SCOPE, nakadiAdminScope);
            scopesForTokens.put(TOKEN_WITH_EVENT_TYPE_WRITE_SCOPE, eventTypeWriteScope);
            scopesForTokens.put(TOKEN_WITH_EVENT_STREAM_READ_SCOPE, eventStreamReadScope);
            scopesForTokens.put(TOKEN_WITH_EVENT_STREAM_WRITE_SCOPE, eventStreamWriteScope);
            scopesForTokens.put(TOKEN_WITH_RANDOM_SCOPE, TestUtils.randomUUID());
            scopesForTokens.put(TOKEN_WITH_REALM, uidScope);
            scopesForTokens.put(TOKEN_WITH_WRONG_REALM, uidScope);
        }

        @PostConstruct
        public void mockRealms() {
            realms.put(TOKEN_WITH_REALM, "/arealm");
            realms.put(TOKEN_WITH_WRONG_REALM, "/wrongrealm");
            realms.put(TOKEN_WITH_UID_SCOPE, "/wrongrealm");
            realms.put(TOKEN_WITH_NAKADI_ADMIN_SCOPE, "/wrongrealm");
            realms.put(TOKEN_WITH_EVENT_TYPE_WRITE_SCOPE, "/wrongrealm");
            realms.put(TOKEN_WITH_EVENT_STREAM_READ_SCOPE, "/wrongrealm");
            realms.put(TOKEN_WITH_EVENT_STREAM_WRITE_SCOPE, "/wrongrealm");
            realms.put(TOKEN_WITH_RANDOM_SCOPE, "/wrongrealm");
        }

        @Bean
        @Primary
        public ResourceServerTokenServices mockResourceTokenServices() {
            final ResourceServerTokenServices tokenServices = mock(ResourceServerTokenServices.class);

            when(tokenServices.loadAuthentication(any())).thenAnswer(invocation -> {

                final UsernamePasswordAuthenticationToken user = new UsernamePasswordAuthenticationToken("user", "N/A",
                        AuthorityUtils.commaSeparatedStringToAuthorityList("ROLE_USER"));

                final String token = (String) invocation.getArguments()[0];
                final Set<String> scopes = ImmutableSet.copyOf(scopesForTokens.get(token));
                final Map<String, Object> details = new HashMap<>();
                details.put("realm", realms.get(token));
                user.setDetails(details);

                final OAuth2Request request = new OAuth2Request(null, null, null, true, scopes, null, null, null, null);

                return new OAuth2Authentication(request, user);
            });
            return tokenServices;
        }

        public ExtendedOAuth2WebSecurityExpressionHandler extendedOAuth2WebSecurityExpressionHandler() {
            return mock(ExtendedOAuth2WebSecurityExpressionHandler.class);
        }

        @Bean
        @Primary
        public DataSource mockDataSource() {
            return mock(DataSource.class);
        }

        @Bean
        @Primary
        public EventTypeRepository mockDbRepository() {
            return mock(EventTypeRepository.class);
        }

        @Bean
        @Primary
        public SubscriptionDbRepository mockSubscriptionDbRepo() {
            return mock(SubscriptionDbRepository.class);
        }

        @Bean
        @Primary
        public EventTypeCache mockEventTypeCache() {
            return mock(EventTypeCache.class);
        }

        @Bean
        @Primary
        public UUIDGenerator mockUuidGenerator() {
            return new UUIDGenerator();
        }

        @Bean
        @Primary
        public FeatureToggleService mockFeatureToggleService() {
            final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
            when(featureToggleService.isFeatureEnabled(any())).thenReturn(true);
            return featureToggleService;
        }

        @Bean
        @Primary
        public ZooKeeperHolder mockZKHolder() {
            return mock(ZooKeeperHolder.class);
        }

        @Bean
        @Primary
        public ChangesRegistry mockChangesRegistry() {
            return mock(ChangesRegistry.class);
        }

        @Bean
        @Primary
        public EventTypeService mockEventTypeService() {
            return mock(EventTypeService.class);
        }

        @Bean
        @Primary
        public TopicRepository mockTopicaRepository() {
            return mock(TopicRepository.class);
        }

        @Bean
        @Primary
        public CursorsService mockCursorsCommitService() {
            return mock(CursorsService.class);
        }

        @Bean
        @Primary
        public EventPublisher mockEventPublisher() {
            return mock(EventPublisher.class);
        }

        @Bean
        @Primary
        public EventTypeMetricRegistry mockEventTypeMetricRegistry() {
            return mock(EventTypeMetricRegistry.class);
        }

        @Bean
        @Primary
        public EventStreamFactory mockEventStreamFactory() {
            return mock(EventStreamFactory.class);
        }

        @Bean
        @Primary
        public ClientResolver mockClientResolver() {
            return mock(ClientResolver.class);
        }

        @Bean
        @Primary
        public KafkaLocationManager mockKafkaLocationManager() {
            return mock(KafkaLocationManager.class);
        }

        @Bean
        @Primary
        public SecuritySettings mockSecuritySettings() {
            final SecuritySettings securitySettings = mock(SecuritySettings.class);
            doReturn(authMode).when(securitySettings).getAuthMode();
            return securitySettings;
        }

        @Bean
        @Primary
        public TimelineSync mockTimelineSync() {
            return mock(TimelineSync.class);
        }

        @Bean
        @Primary
        public TopicRepositoryHolder mockTopicRepositoryHolder() {
            return mock(TopicRepositoryHolder.class);
        }

        @Bean
        @Primary
        public StorageDbRepository mockStorageDbRepository() {
            final StorageDbRepository storageDbRepository = mock(StorageDbRepository.class);
            when(storageDbRepository.getStorage("default")).thenReturn(Optional.empty());
            return storageDbRepository;
        }

        @Bean
        @Primary
        public MessageDigest mockSha256MessageDigest() {
            return mock(MessageDigest.class);
        }

        @Bean
        @Primary
        public PartitionsCalculator mockPartitionsCalculator() {
            return mock(PartitionsCalculator.class);
        }
    }

    protected static final ResultMatcher STATUS_NOT_401_OR_403 = status().is(not(
            isOneOf(UNAUTHORIZED.value(), FORBIDDEN.value())));

    protected static final String TOKEN_WITH_UID_SCOPE = TestUtils.randomUUID();
    protected static final String TOKEN_WITH_NAKADI_ADMIN_SCOPE = TestUtils.randomUUID();
    protected static final String TOKEN_WITH_EVENT_TYPE_WRITE_SCOPE = TestUtils.randomUUID();
    protected static final String TOKEN_WITH_EVENT_STREAM_READ_SCOPE = TestUtils.randomUUID();
    protected static final String TOKEN_WITH_EVENT_STREAM_WRITE_SCOPE = TestUtils.randomUUID();
    protected static final String TOKEN_WITH_RANDOM_SCOPE = TestUtils.randomUUID();
    protected static final String TOKEN_WITH_REALM = TestUtils.randomUUID();
    protected static final String TOKEN_WITH_WRONG_REALM = TestUtils.randomUUID();

    protected static SecuritySettings.AuthMode authMode;

    protected static final List<Endpoint> ENDPOINTS = ImmutableList.of(
            new Endpoint(POST, "/event-types", TOKEN_WITH_EVENT_TYPE_WRITE_SCOPE),
            new Endpoint(PUT, "/event-types/foo", TOKEN_WITH_EVENT_TYPE_WRITE_SCOPE),
            new Endpoint(DELETE, "/event-types/foo", TOKEN_WITH_NAKADI_ADMIN_SCOPE),
            new Endpoint(POST, "/event-types/foo/events", TOKEN_WITH_EVENT_STREAM_WRITE_SCOPE),
            new Endpoint(GET, "/event-types/foo/events", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/event-types/foo/partitions", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/event-types/foo/partitions/bar", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(POST, "/event-types/foo/shifted-cursors", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(POST, "/event-types/foo/cursors-lag", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(POST, "/event-types/foo/cursor-distances", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/subscriptions/foo/events", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(POST, "/subscriptions", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/subscriptions", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(POST, "/subscriptions/foo/cursors", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/subscriptions/foo/cursors", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/subscriptions/foo", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(DELETE, "/subscriptions/foo", TOKEN_WITH_EVENT_STREAM_READ_SCOPE),
            new Endpoint(GET, "/subscriptions/foo/stats", TOKEN_WITH_EVENT_STREAM_READ_SCOPE));

    protected static final List<Endpoint> ENDPOINTS_FOR_UID_SCOPE = ImmutableList.of(
            new Endpoint(GET, "/metrics", TOKEN_WITH_UID_SCOPE),
            new Endpoint(GET, "/registry/partition-strategies", TOKEN_WITH_UID_SCOPE),
            new Endpoint(GET, "/event-types", TOKEN_WITH_UID_SCOPE),
            new Endpoint(GET, "/event-types/foo", TOKEN_WITH_UID_SCOPE),
            new Endpoint(GET, "/version", TOKEN_WITH_UID_SCOPE));

    protected static final List<Endpoint> ENDPOINTS_FOR_REALM = ImmutableList.of(
            new Endpoint(POST, "/event-types", TOKEN_WITH_REALM),
            new Endpoint(PUT, "/event-types/foo", TOKEN_WITH_REALM),
            new Endpoint(DELETE, "/event-types/foo", TOKEN_WITH_REALM),
            new Endpoint(POST, "/event-types/foo/events", TOKEN_WITH_REALM),
            new Endpoint(GET, "/event-types/foo/events", TOKEN_WITH_REALM),
            new Endpoint(GET, "/event-types/foo/partitions", TOKEN_WITH_REALM),
            new Endpoint(GET, "/event-types/foo/partitions/bar", TOKEN_WITH_REALM),
            new Endpoint(POST, "/event-types/foo/shifted-cursors", TOKEN_WITH_REALM),
            new Endpoint(POST, "/event-types/foo/cursors-lag", TOKEN_WITH_REALM),
            new Endpoint(POST, "/event-types/foo/cursor-distances", TOKEN_WITH_REALM),
            new Endpoint(GET, "/subscriptions/foo/events", TOKEN_WITH_REALM),
            new Endpoint(POST, "/subscriptions", TOKEN_WITH_REALM),
            new Endpoint(GET, "/subscriptions", TOKEN_WITH_REALM),
            new Endpoint(POST, "/subscriptions/foo/cursors", TOKEN_WITH_REALM),
            new Endpoint(GET, "/subscriptions/foo/cursors", TOKEN_WITH_REALM),
            new Endpoint(GET, "/subscriptions/foo", TOKEN_WITH_REALM),
            new Endpoint(DELETE, "/subscriptions/foo", TOKEN_WITH_REALM),
            new Endpoint(GET, "/subscriptions/foo/stats", TOKEN_WITH_REALM),
            new Endpoint(GET, "/metrics", TOKEN_WITH_REALM),
            new Endpoint(GET, "/registry/partition-strategies", TOKEN_WITH_REALM),
            new Endpoint(GET, "/event-types", TOKEN_WITH_REALM),
            new Endpoint(GET, "/event-types/foo", TOKEN_WITH_REALM),
            new Endpoint(GET, "/version", TOKEN_WITH_REALM));

    private static final List<Endpoint> ENDPOINTS_WITH_NO_SCOPE = ImmutableList.of(
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
        ENDPOINTS_WITH_NO_SCOPE.forEach(this::checkOkResponse);
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
