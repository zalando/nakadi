package org.zalando.nakadi.controller;

import java.io.IOException;
import java.util.Optional;
import javax.ws.rs.core.Response;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeResource;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;

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
        final Resource resource = new EventTypeResource(eventType.getName(), eventType.getAuthorization());

        doReturn(eventType).when(eventTypeRepository).findByName(any());
        doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN, resource))
                .when(authorizationValidator).authorizeEventTypeAdmin(eventType);

        putEventType(eventType, eventType.getName())
                .andExpect(status().isForbidden())
                .andExpect(content().string(matchesProblem(Problem.valueOf(Response.Status.FORBIDDEN,
                        "Access on ADMIN event-type:"+ eventType.getName() + " denied"))));
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
                .andExpect(content().string(matchesProblem(Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY,
                        "Changing authorization object to `null` is not possible due to existing one"))));
    }

    @Test
    public void whenDELETENotAuthorized200() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        final Resource resource = new EventTypeResource(eventType.getName(), eventType.getAuthorization());

        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(any());
        doThrow(new AccessDeniedException(AuthorizationService.Operation.ADMIN, resource))
                .when(authorizationValidator).authorizeEventTypeAdmin(eventType);

        deleteEventType(eventType.getName())
                .andExpect(status().isForbidden())
                .andExpect(content().string(matchesProblem(Problem.valueOf(Response.Status.FORBIDDEN,
                        "Access on ADMIN event-type:" + eventType.getName() + " denied"))));
    }

}
