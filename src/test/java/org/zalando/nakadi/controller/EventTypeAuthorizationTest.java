package org.zalando.nakadi.controller;

import java.io.IOException;
import javax.ws.rs.core.Response;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.ForbiddenAccessException;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
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
    public void whenPUTNotAuthorized200() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();

        doReturn(eventType).when(eventTypeRepository).findByName(any());
        doThrow(new ForbiddenAccessException("Updating the `EventType` is only allowed for clients that " +
                "satisfy the authorization `admin` requirements"))
                .when(authorizationValidator).authorizeEventTypeAdmin(eventType);

        putEventType(eventType, eventType.getName())
                .andExpect(status().isForbidden())
                .andExpect(content().string(matchesProblem(Problem.valueOf(Response.Status.FORBIDDEN,
                        "Updating the `EventType` is only allowed for clients that satisfy the authorization " +
                                "`admin` requirements"))));
    }

}
