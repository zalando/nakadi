package org.zalando.nakadi.controller;

import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeOptions;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.problem.Problem;

import java.io.IOException;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

public class EventTypeAuthorizationTest extends EventTypeControllerTestCase {

    public EventTypeAuthorizationTest() throws IOException {
    }

    @Test
    public void whenPUTAuthorized200() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();

        doReturn(eventType).when(eventTypeRepository).findByName(any());

        putEventType(eventType, eventType.getName())
                .andExpect(status().isOk());
    }

    @Test
    public void whenPUTNotAuthorizedThen403() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        final Resource resource = eventType.asResource();

        doReturn(eventType).when(eventTypeRepository).findByName(any());
        doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN, resource))
                .when(authorizationValidator).authorizeEventTypeAdmin(eventType);

        putEventType(eventType, eventType.getName())
                .andExpect(status().isForbidden())
                .andExpect(content().string(matchesProblem(Problem.valueOf(FORBIDDEN,
                        "Access on ADMIN event-type:" + eventType.getName() + " denied"))));
    }

    @Test
    public void whenPUTUnlimitedRetentionTimeByAdminThen200() throws Exception {
        final EventTypeOptions eto = new EventTypeOptions();
        eto.setRetentionTime(Long.MAX_VALUE);
        final EventType eventType = EventTypeTestBuilder.builder().options(eto).build();

        doReturn(eventType).when(eventTypeRepository).findByName(any());
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(true);

        putEventType(eventType, eventType.getName())
                .andExpect(status().isOk());
    }

    @Test
    public void whenPUTUnlimitedRetentionTimeByUserThen422() throws Exception {
        final EventTypeOptions eto = new EventTypeOptions();
        eto.setRetentionTime(Long.MAX_VALUE);
        final EventType eventType = EventTypeTestBuilder.builder().options(eto).build();

        doReturn(eventType).when(eventTypeRepository).findByName(any());
        when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(false);

        putEventType(eventType, eventType.getName())
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().string(matchesProblem(Problem.valueOf(UNPROCESSABLE_ENTITY,
                        "Field \"options.retention_time\" can not be more than 345600000"))));
    }

    @Test
    public void whenPUTNullAuthorizationForExistingAuthorization() throws Exception {
        final EventType newEventType = EventTypeTestBuilder.builder().build();
        doReturn(newEventType).when(eventTypeRepository).findByName(any());
        doThrow(new UnableProcessException(
                "Changing authorization object to `null` is not possible due to existing one"))
                .when(authorizationValidator).validateAuthorization(any(), any());

        putEventType(newEventType, newEventType.getName())
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().string(matchesProblem(Problem.valueOf(UNPROCESSABLE_ENTITY,
                        "Changing authorization object to `null` is not possible due to existing one"))));
    }

    @Test
    public void whenDELETENotAuthorizedThen403() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        final Resource resource = eventType.asResource();

        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(any());
        doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN, resource))
                .when(authorizationValidator).authorizeEventTypeAdmin(eventType);

        deleteEventType(eventType.getName())
                .andExpect(status().isForbidden())
                .andExpect(content().string(matchesProblem(Problem.valueOf(FORBIDDEN,
                        "Access on ADMIN event-type:" + eventType.getName() + " denied"))));
    }

}
