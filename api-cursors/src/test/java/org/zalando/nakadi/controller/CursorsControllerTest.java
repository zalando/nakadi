package org.zalando.nakadi.controller;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.controller.advice.CursorsExceptionHandler;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorTokenService;
import org.zalando.nakadi.service.CursorsService;
import org.zalando.nakadi.service.EventStreamChecks;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.CursorCommitResult;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.problem.Problem;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.patch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

public class CursorsControllerTest {

    private static final String PROBLEM_CONTENT_TYPE = "application/problem+json";
    private static final String SUBSCRIPTION_ID = "my-sub";

    private static final String MY_ET = "my-et";
    private static final String TOKEN = "cursor-token";

    private static final Timeline TIMELINE = TestUtils.buildTimelineWithTopic(MY_ET);

    private static final ImmutableList<NakadiCursor> DUMMY_NAKADI_CURSORS = ImmutableList.of(
            NakadiCursor.of(TIMELINE, "0", "000000000000000010"),
            NakadiCursor.of(TIMELINE, "1", "000000000000000010")
    );

    private static final ImmutableList<SubscriptionCursor> DUMMY_CURSORS = ImmutableList.of(
            new SubscriptionCursor("0", "10", MY_ET, TOKEN),
            new SubscriptionCursor("1", "10", MY_ET, TOKEN));

    private final CursorsService cursorsService = mock(CursorsService.class);
    private final MockMvc mockMvc;
    private final SubscriptionDbRepository subscriptionRepository;
    private final CursorConverter cursorConverter;
    private final AuthorizationService authorizationService;
    private final Client client;
    private final EventStreamChecks eventStreamChecks;

    public CursorsControllerTest() throws Exception {

        subscriptionRepository = mock(SubscriptionDbRepository.class);
        authorizationService = mock(AuthorizationService.class);
        when(authorizationService.getSubject()).thenReturn(Optional.empty());
        cursorConverter = mock(CursorConverter.class);

        IntStream.range(0, DUMMY_CURSORS.size()).forEach(idx ->
                when(cursorConverter.convert(eq(DUMMY_NAKADI_CURSORS.get(idx)), any()))
                        .thenReturn(DUMMY_CURSORS.get(idx)));

        final EventTypeCache eventTypeCache = mock(EventTypeCache.class);
        doReturn(TestUtils.buildDefaultEventType()).when(eventTypeCache).getEventType(any());
        doReturn(RandomSubscriptionBuilder.builder().build()).when(subscriptionRepository).getSubscription(any());
        final CursorTokenService tokenService = mock(CursorTokenService.class);
        when(tokenService.generateToken()).thenReturn(TOKEN);

        client = mock(Client.class);

        eventStreamChecks = mock(EventStreamChecks.class);

        final CursorsController controller = new CursorsController(cursorsService, cursorConverter, tokenService,
                eventStreamChecks);

        final SecuritySettings settings = mock(SecuritySettings.class);
        doReturn(SecuritySettings.AuthMode.OFF).when(settings).getAuthMode();

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(settings, authorizationService))
                .setControllerAdvice(new NakadiProblemExceptionHandler(), new CursorsExceptionHandler())
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
                        new SubscriptionCursor(null, null, null, null), "")));

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY, "partition must not be null");

        checkForProblem(postCursors(DUMMY_CURSORS), expectedProblem);
    }

    @Test
    public void whenBodyIsNotJsonThenBadRequest() throws Exception {
        postCursorsString("blah")
                .andExpect(status().is(HttpStatus.BAD_REQUEST.value()));
    }

    @Test
    public void whenCommitCursorWithoutEventTypeThenUnprocessableEntity() throws Exception {
        checkForProblem(
                postCursorsString("{\"items\":[{\"offset\":\"0\",\"partition\":\"0\",\"cursor_token\":\"x\"}]}"),
                TestUtils.invalidProblem("items[0].event_type", "must not be null"));
    }

    @Test
    public void whenCommitCursorWithEmptyPartitionThenUnprocessableEntity() throws Exception {
        checkForProblem(
                postCursorsString("{\"items\":[{\"offset\":\"0\",\"partition\":\"\",\"cursor_token\":\"x\"," +
                        "\"event_type\":\"et\"}]}"),
                TestUtils.invalidProblem("items[0].partition", "cursor partition cannot be empty"));
    }

    @Test
    public void whenCommitCursorWithEmptyOffsetThenUnprocessableEntity() throws Exception {
        checkForProblem(
                postCursorsString("{\"items\":[{\"offset\":\"\",\"partition\":\"0\",\"cursor_token\":\"x\"," +
                        "\"event_type\":\"et\"}]}"),
                TestUtils.invalidProblem("items[0].offset", "cursor offset cannot be empty"));
    }

    @Test
    public void whenSubscriptionConsumptionBlockedThenAccessDeniedOnPostCommit() throws Exception {
        Mockito.when(eventStreamChecks.isSubscriptionConsumptionBlocked(anyString(), any())).thenReturn(true);
        final Problem expectedProblem = Problem.valueOf(FORBIDDEN, "Application or subscription is blocked");
        checkForProblem(
                postCursorsString("{\"items\":[{\"offset\":\"0\",\"partition\":\"0\",\"cursor_token\":\"x\"," +
                        "\"event_type\":\"et\"}]}"),
                expectedProblem);
    }

    @Test
    public void whenSubscriptionConsumptionBlockedThenAccessDeniedOnPatchCommit() throws Exception {
        Mockito.when(eventStreamChecks.isSubscriptionConsumptionBlocked(anyString(), any())).thenReturn(true);
        final Problem expectedProblem = Problem.valueOf(FORBIDDEN, "Application or subscription is blocked");
        checkForProblem(
                patchCursorsString("{\"items\":[{\"offset\":\"0\",\"partition\":\"0\",\"cursor_token\":\"x\"," +
                        "\"event_type\":\"et\"}]}"),
                expectedProblem);
    }

    @Test
    public void whenSubscriptionConsumptionBlockedThenAccessDeniedOnGetCursors() throws Exception {
        Mockito.when(eventStreamChecks.isSubscriptionConsumptionBlocked(anyString(), any())).thenReturn(true);
        final Problem expectedProblem = Problem.valueOf(FORBIDDEN, "Application or subscription is blocked");
        checkForProblem(
                getCursors(),
                expectedProblem);
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

    private ResultActions patchCursorsString(final String cursors) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = patch("/subscriptions/" + SUBSCRIPTION_ID + "/cursors")
                .header("X-Nakadi-StreamId", "test-stream-id")
                .contentType(APPLICATION_JSON)
                .content(cursors);
        return mockMvc.perform(requestBuilder);
    }

    private ResultActions postCursorsString(final String cursors) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = post("/subscriptions/" + SUBSCRIPTION_ID + "/cursors")
                .header("X-Nakadi-StreamId", "test-stream-id")
                .contentType(APPLICATION_JSON)
                .content(cursors);
        return mockMvc.perform(requestBuilder);
    }

}
