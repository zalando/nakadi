package org.zalando.nakadi.controller;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.CursorsService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.CursorCommitResult;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.problem.Problem;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.service.FeatureToggleService.Feature.HIGH_LEVEL_API;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.buildTimelineWithTopic;
import static org.zalando.nakadi.utils.TestUtils.invalidProblem;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

public class CursorsControllerTest {

    private static final String PROBLEM_CONTENT_TYPE = "application/problem+json";
    private static final String SUBSCRIPTION_ID = "my-sub";

    private static final String MY_ET = "my-et";
    private static final String TOKEN = "cursor-token";

    private static final Timeline TIMELINE = buildTimelineWithTopic(MY_ET);

    private static final ImmutableList<NakadiCursor> DUMMY_NAKADI_CURSORS = ImmutableList.of(
            NakadiCursor.of(TIMELINE, "0", "000000000000000010"),
            NakadiCursor.of(TIMELINE, "1", "000000000000000010")
    );

    private static final ImmutableList<SubscriptionCursor> DUMMY_CURSORS = ImmutableList.of(
            new SubscriptionCursor("0", "10", MY_ET, TOKEN),
            new SubscriptionCursor("1", "10", MY_ET, TOKEN));

    private final CursorsService cursorsService = mock(CursorsService.class);
    private final MockMvc mockMvc;
    private final FeatureToggleService featureToggleService;
    private final SubscriptionDbRepository subscriptionRepository;
    private final CursorConverter cursorConverter;

    public CursorsControllerTest() throws Exception {

        featureToggleService = mock(FeatureToggleService.class);
        when(featureToggleService.isFeatureEnabled(any())).thenReturn(true);

        subscriptionRepository = mock(SubscriptionDbRepository.class);
        cursorConverter = mock(CursorConverter.class);

        IntStream.range(0, DUMMY_CURSORS.size()).forEach(idx ->
                when(cursorConverter.convert(eq(DUMMY_NAKADI_CURSORS.get(idx)), any()))
                        .thenReturn(DUMMY_CURSORS.get(idx)));

        final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
        doReturn(buildDefaultEventType()).when(eventTypeRepository).findByName(any());
        doReturn(RandomSubscriptionBuilder.builder().build()).when(subscriptionRepository).getSubscription(any());
        final CursorTokenService tokenService = mock(CursorTokenService.class);
        when(tokenService.generateToken()).thenReturn(TOKEN);

        final CursorsController controller = new CursorsController(cursorsService, featureToggleService,
                cursorConverter, tokenService);

        final SecuritySettings settings = mock(SecuritySettings.class);
        doReturn(SecuritySettings.AuthMode.OFF).when(settings).getAuthMode();
        doReturn("nakadi").when(settings).getAdminClientId();

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(settings, featureToggleService))
                .setControllerAdvice(new NakadiProblemControllerAdvice())
                .build();
    }

    @Test
    public void whenCommitValidCursorsThenNoContent() throws Exception {
        when(cursorsService.commitCursors(any(), any(), any()))
                .thenReturn(ImmutableList.of());
        postCursors(DUMMY_CURSORS)
                .andExpect(status().isNoContent());
    }

    @Test
    public void whenCommitInvalidCursorsThenOk() throws Exception {
        when(cursorsService.commitCursors(any(), any(), any()))
                .thenReturn(DUMMY_CURSORS.stream().map(v -> Boolean.FALSE).collect(Collectors.toList()));
        final ItemsWrapper<CursorCommitResult> expectation = new ItemsWrapper<>(
                DUMMY_CURSORS.stream()
                        .map(c -> new CursorCommitResult(c, false))
                        .collect(Collectors.toList()));
        postCursors(DUMMY_CURSORS)
                .andExpect(status().isOk())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectation)));
    }

    @Test
    public void whenNoSubscriptionThenNotFound() throws Exception {
        when(cursorsService.commitCursors(any(), eq(SUBSCRIPTION_ID), any()))
                .thenThrow(new NoSuchSubscriptionException("dummy-message"));
        final Problem expectedProblem = Problem.valueOf(NOT_FOUND, "dummy-message");

        checkForProblem(postCursors(DUMMY_CURSORS), expectedProblem);
    }

    @Test
    public void whenNoEventTypeThenUnprocessableEntity() throws Exception {
        when(cursorsService.commitCursors(any(), any(), any()))
                .thenThrow(new NoSuchEventTypeException("dummy-message"));
        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY, "dummy-message");

        checkForProblem(postCursors(DUMMY_CURSORS), expectedProblem);
    }

    @Test
    public void whenServiceUnavailableExceptionThenServiceUnavailable() throws Exception {
        when(cursorsService.commitCursors(any(), any(), any()))
                .thenThrow(new ServiceTemporarilyUnavailableException("dummy-message"));
        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, "dummy-message");

        checkForProblem(postCursors(DUMMY_CURSORS), expectedProblem);
    }

    @Test
    public void whenInvalidCursorExceptionThenUnprocessableEntity() throws Exception {
        when(cursorsService.commitCursors(any(), any(), any()))
                .thenThrow((new InvalidCursorException(CursorError.NULL_PARTITION,
                        new SubscriptionCursor(null, null, null, null))));

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY, "partition must not be null");

        checkForProblem(postCursors(DUMMY_CURSORS), expectedProblem);
    }

    @Test
    public void whenBodyIsNotJsonThenBadRequest() throws Exception {
        postCursorsString("blah")
                .andExpect(status().is(HttpStatus.BAD_REQUEST.value()));
    }

    @Test
    public void whenGetAndNoFeatureThenNotImplemented() throws Exception {
        Mockito.doThrow(new FeatureNotAvailableException("Not available", HIGH_LEVEL_API))
                .when(featureToggleService).checkFeatureOn(eq(HIGH_LEVEL_API));
        getCursors().andExpect(status().is(HttpStatus.NOT_IMPLEMENTED.value()));
    }

    @Test
    public void whenCommitCursorWithoutEventTypeThenUnprocessableEntity() throws Exception {
        checkForProblem(
                postCursorsString("{\"items\":[{\"offset\":\"0\",\"partition\":\"0\",\"cursor_token\":\"x\"}]}"),
                invalidProblem("items[0].event_type", "may not be null"));
    }

    private ResultActions getCursors() throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = get("/subscriptions/" + SUBSCRIPTION_ID + "/cursors");
        return mockMvc.perform(requestBuilder);
    }

    private void checkForProblem(final ResultActions resultActions, final Problem expectedProblem) throws Exception {
        resultActions
                .andExpect(status().is(expectedProblem.getStatus().getStatusCode()))
                .andExpect(content().contentType(PROBLEM_CONTENT_TYPE))
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    private ResultActions postCursors(final List<SubscriptionCursor> cursors) throws Exception {
        final ItemsWrapper<SubscriptionCursor> cursorsWrapper = new ItemsWrapper<>(cursors);
        return postCursorsString(TestUtils.OBJECT_MAPPER.writeValueAsString(cursorsWrapper));
    }

    private ResultActions postCursorsString(final String cursors) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = post("/subscriptions/" + SUBSCRIPTION_ID + "/cursors")
                .header("X-Nakadi-StreamId", "test-stream-id")
                .contentType(APPLICATION_JSON)
                .content(cursors);
        return mockMvc.perform(requestBuilder);
    }

}
