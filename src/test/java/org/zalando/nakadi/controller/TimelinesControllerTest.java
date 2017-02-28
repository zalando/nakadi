package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.ForbiddenAccessException;
import org.zalando.nakadi.exceptions.NotFoundException;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.util.PrincipalMockFactory;
import org.zalando.nakadi.view.TimelineView;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;


public class TimelinesControllerTest {

    private final TimelineService timelineService = Mockito.mock(TimelineService.class);
    private final SecuritySettings securitySettings = Mockito.mock(SecuritySettings.class);
    private final FeatureToggleService featureToggleService = Mockito.mock(FeatureToggleService.class);
    private final ObjectMapper objectMapper;
    private MockMvc mockMvc;

    public TimelinesControllerTest() {
        final TimelinesController controller = new TimelinesController(timelineService);
        objectMapper = new JsonConfig().jacksonObjectMapper();
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(),
                        new MappingJackson2HttpMessageConverter(objectMapper))
                .setCustomArgumentResolvers(new ClientResolver(securitySettings, featureToggleService))
                .build();
    }

    @Test
    public void whenPostTimelineThenCreated() throws Exception {
        Mockito.doNothing().when(timelineService).createTimeline(Mockito.any(), Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.post("/event-types/event_type/timelines")
                .contentType(MediaType.APPLICATION_JSON)
                .content(new JSONObject().put("storage_id", "default").toString())
                .principal(PrincipalMockFactory.mockPrincipal("nakadi")))
                .andExpect(MockMvcResultMatchers.status().isCreated());
    }

    @Test
    public void whenGetTimelinesThenOk() throws Exception {
        final Storage kafkaStorage = StoragesControllerTest.createKafkaStorage("deafult");
        final ImmutableList<Timeline> timelines = ImmutableList.of(
                Timeline.createTimeline("event_type", 0, kafkaStorage, "topic", new Date()),
                Timeline.createTimeline("event_type_1", 1, kafkaStorage, "topic_1", new Date()));
        Mockito.when(timelineService.getTimelines(Mockito.any(), Mockito.any())).thenReturn(timelines);
        final List<TimelineView> timelineViews = timelines.stream().map(TimelineView::new).collect(Collectors.toList());

        mockMvc.perform(MockMvcRequestBuilders.get("/event-types/event_type/timelines")
                .contentType(MediaType.APPLICATION_JSON)
                .principal(PrincipalMockFactory.mockPrincipal("nakadi")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().json(objectMapper.writeValueAsString(timelineViews)));
    }

    @Test
    public void whenDeleteTimelineThenOk() throws Exception {
        Mockito.doNothing().when(timelineService).delete(Mockito.any(), Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.delete("/event-types/event_type/timelines/timeli-uuid")
                .contentType(MediaType.APPLICATION_JSON)
                .principal(PrincipalMockFactory.mockPrincipal("nakadi")))
                .andExpect(MockMvcResultMatchers.status().isOk());
    }

    @Test
    public void whenForbiddenAccessExceptionThen403() throws Exception {
        Mockito.doThrow(new ForbiddenAccessException("whenForbiddenAccessExceptionThen403"))
                .when(timelineService).delete(Mockito.any(), Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.delete("/event-types/event_type/timelines/timeli-uuid")
                .contentType(MediaType.APPLICATION_JSON)
                .principal(PrincipalMockFactory.mockPrincipal("nakadi")))
                .andExpect(MockMvcResultMatchers.status().isForbidden())
                .andExpect(MockMvcResultMatchers.content().json(objectMapper.writeValueAsString(
                        Problem.valueOf(Response.Status.FORBIDDEN, "whenForbiddenAccessExceptionThen403"))));
    }

    @Test
    public void whenNotFoundExceptionThen404() throws Exception {
        Mockito.doThrow(new NotFoundException("whenNotFoundExceptionThen404"))
                .when(timelineService).delete(Mockito.any(), Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.delete("/event-types/event_type/timelines/timeli-uuid")
                .contentType(MediaType.APPLICATION_JSON)
                .principal(PrincipalMockFactory.mockPrincipal("nakadi")))
                .andExpect(MockMvcResultMatchers.status().isNotFound())
                .andExpect(MockMvcResultMatchers.content().json(objectMapper.writeValueAsString(
                        Problem.valueOf(Response.Status.NOT_FOUND, "whenNotFoundExceptionThen404"))));
    }

    @Test
    public void whenUnableProcessExceptionThen422() throws Exception {
        Mockito.doThrow(new UnableProcessException("whenUnableProcessExceptionThen422"))
                .when(timelineService).delete(Mockito.any(), Mockito.any(), Mockito.any());
        mockMvc.perform(MockMvcRequestBuilders.delete("/event-types/event_type/timelines/timeli-uuid")
                .contentType(MediaType.APPLICATION_JSON)
                .principal(PrincipalMockFactory.mockPrincipal("nakadi")))
                .andExpect(MockMvcResultMatchers.status().isUnprocessableEntity())
                .andExpect(MockMvcResultMatchers.content().json(objectMapper.writeValueAsString(
                        Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY, "whenUnableProcessExceptionThen422"))));
    }

}
