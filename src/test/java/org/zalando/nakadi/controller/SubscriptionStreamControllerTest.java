package org.zalando.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.security.FullAccessClient;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.ClosedConnectionsCrutch;
import org.zalando.nakadi.service.EventTypeChangeListener;
import org.zalando.nakadi.service.subscription.SubscriptionStreamerFactory;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.problem.Problem;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.HIGH_LEVEL_API;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;

public class SubscriptionStreamControllerTest {

    private static final Client FULL_ACCESS_CLIENT = new FullAccessClient("clientId");

    private HttpServletRequest requestMock;
    private HttpServletResponse responseMock;

    private SubscriptionStreamController controller;
    private JsonTestHelper jsonHelper;

    private SubscriptionStreamerFactory subscriptionStreamerFactory;

    private SubscriptionDbRepository subscriptionDbRepository;
    private EventTypeRepository eventTypeRepository;
    private AuthorizationValidator authorizationValidator;
    private EventTypeChangeListener eventTypeChangeListener;

    @Before
    public void setup() throws NakadiException, UnknownHostException, InvalidCursorException {
        final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();
        jsonHelper = new JsonTestHelper(objectMapper);

        requestMock = mock(HttpServletRequest.class);
        responseMock = mock(HttpServletResponse.class);

        final MetricRegistry metricRegistry = mock(MetricRegistry.class);
        when(metricRegistry.counter(any())).thenReturn(mock(Counter.class));

        final ClosedConnectionsCrutch crutch = mock(ClosedConnectionsCrutch.class);
        when(crutch.listenForConnectionClose(requestMock)).thenReturn(new AtomicBoolean(true));

        final BlacklistService blacklistService = Mockito.mock(BlacklistService.class);
        Mockito.when(blacklistService.isSubscriptionConsumptionBlocked(any(String.class), any(String.class)))
                .thenReturn(false);

        final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
        when(featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)).thenReturn(true);

        final NakadiSettings nakadiSettings = mock(NakadiSettings.class);

        subscriptionStreamerFactory = mock(SubscriptionStreamerFactory.class);
        subscriptionDbRepository = mock(SubscriptionDbRepository.class);
        eventTypeRepository = mock(EventTypeRepository.class);
        authorizationValidator = mock(AuthorizationValidator.class);
        eventTypeChangeListener = mock(EventTypeChangeListener.class);

        controller = new SubscriptionStreamController(subscriptionStreamerFactory, featureToggleService, objectMapper,
                crutch, nakadiSettings, blacklistService, metricRegistry, subscriptionDbRepository);
    }

    @Test
    public void whenBatchLimitLowerThan1ThenUnprocessableEntity() throws Exception {
        final StreamingResponseBody responseBody = controller.streamEvents("abc", 0, 0, null, 10, null, null,
                requestMock, responseMock, FULL_ACCESS_CLIENT);

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY, "batch_limit can't be lower than 1");
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    protected String responseToString(final StreamingResponseBody responseBody) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        responseBody.writeTo(out);
        return out.toString();
    }

}
