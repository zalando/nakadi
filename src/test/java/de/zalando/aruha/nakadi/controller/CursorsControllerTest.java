package de.zalando.aruha.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.CursorError;
import de.zalando.aruha.nakadi.exceptions.InvalidCursorException;
import de.zalando.aruha.nakadi.exceptions.NoSuchSubscriptionException;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.service.CursorsCommitService;
import de.zalando.aruha.nakadi.util.FeatureToggleService;
import de.zalando.aruha.nakadi.utils.JsonTestHelper;
import org.junit.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.zalando.problem.Problem;

import java.util.List;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;

public class CursorsControllerTest {

    private static final String PROBLEM_CONTENT_TYPE = "application/problem+json";
    private static final String SUBSCRIPTION_ID = "my-sub";

    private static final ImmutableList<Cursor> DUMMY_CURSORS =
            ImmutableList.of(new Cursor("0", "10"), new Cursor("1", "10"));

    private final CursorsCommitService cursorsCommitService = mock(CursorsCommitService.class);
    private final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();
    private final MockMvc mockMvc;
    private final JsonTestHelper jsonHelper;

    public CursorsControllerTest() throws Exception {
        jsonHelper = new JsonTestHelper(objectMapper);

        final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
        when(featureToggleService.isFeatureEnabled(any())).thenReturn(true);

        final CursorsController controller = new CursorsController(cursorsCommitService, featureToggleService);
        final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter =
                new MappingJackson2HttpMessageConverter(objectMapper);

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), jackson2HttpMessageConverter)
                .build();
    }

    @Test
    public void whenCommitValidCursorsThenOk() throws Exception {
        when(cursorsCommitService.commitCursors(any(), any())).thenReturn(true);
        putCursors(DUMMY_CURSORS)
                .andExpect(status().isOk());
    }

    @Test
    public void whenCommitOldCursorsThenNoContent() throws Exception {
        when(cursorsCommitService.commitCursors(any(), any())).thenReturn(false);
        putCursors(DUMMY_CURSORS)
                .andExpect(status().isNoContent());
    }

    @Test
    public void whenNoSubscriptionThenNotFound() throws Exception {
        when(cursorsCommitService.commitCursors(any(), any()))
                .thenThrow(new NoSuchSubscriptionException("dummy-message"));
        final Problem expectedProblem = Problem.valueOf(NOT_FOUND, "dummy-message");

        checkForProblem(putCursors(DUMMY_CURSORS), expectedProblem);
    }

    @Test
    public void whenServiceUnavailableExceptionThenServiceUnavailable() throws Exception {
        when(cursorsCommitService.commitCursors(any(), any()))
                .thenThrow(new ServiceUnavailableException("dummy-message"));
        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, "dummy-message");

        checkForProblem(putCursors(DUMMY_CURSORS), expectedProblem);
    }

    @Test
    public void whenInvalidCursorExceptionThenUnprocessableEntity() throws Exception {
        when(cursorsCommitService.commitCursors(any(), any()))
                .thenThrow((new InvalidCursorException(CursorError.NULL_PARTITION, new Cursor(null, null))));

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY, "partition must not be null");

        checkForProblem(putCursors(DUMMY_CURSORS), expectedProblem);
    }

    @Test
    public void whenBodyIsNotJsonThenBadRequest() throws Exception {
        putCursorsString("blah")
                .andExpect(status().is(HttpStatus.BAD_REQUEST.value()));
    }

    private void checkForProblem(final ResultActions resultActions, final Problem expectedProblem) throws Exception {
        resultActions
                .andExpect(status().is(expectedProblem.getStatus().getStatusCode()))
                .andExpect(content().contentType(PROBLEM_CONTENT_TYPE))
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    private ResultActions putCursors(final List<Cursor> cursors) throws Exception {
        return putCursorsString(objectMapper.writeValueAsString(cursors));
    }

    private ResultActions putCursorsString(final String cursors) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = put("/subscriptions/" + SUBSCRIPTION_ID + "/cursors")
                .contentType(APPLICATION_JSON)
                .content(cursors);
        return mockMvc.perform(requestBuilder);
    }

}