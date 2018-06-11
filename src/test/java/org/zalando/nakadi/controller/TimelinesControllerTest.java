package org.zalando.nakadi.controller;

import com.google.common.collect.ImmutableList;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.MediaType;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.util.PrincipalMockFactory;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.TimelineView;

import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.when;
import static org.zalando.nakadi.config.SecuritySettings.AuthMode.OFF;


public class TimelinesControllerTest {

    private final TimelineService timelineService = Mockito.mock(TimelineService.class);
    private final SecuritySettings securitySettings = Mockito.mock(SecuritySettings.class);
    private MockMvc mockMvc;

    public TimelinesControllerTest() {
        final TimelinesController controller = new TimelinesController(timelineService);
        when(securitySettings.getAuthMode()).thenReturn(OFF);
        when(securitySettings.getAdminClientId()).thenReturn("nakadi");
        final FeatureToggleService featureToggleService = Mockito.mock(FeatureToggleService.class);
        mockMvc = MockMvcBuilders.standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(securitySettings, featureToggleService))
                .setControllerAdvice(new NakadiProblemControllerAdvice())
                .build();
    }

    @Test
    public void whenPostTimelineThenCreated() throws Exception {
        Mockito.doNothing().when(timelineService).createTimeline(Mockito.any(), Mockito.any());
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
        Mockito.when(timelineService.getTimelines(Mockito.any())).thenReturn(timelines);
        final List<TimelineView> timelineViews = timelines.stream().map(TimelineView::new).collect(Collectors.toList());

        mockMvc.perform(MockMvcRequestBuilders.get("/event-types/event_type/timelines")
                .contentType(MediaType.APPLICATION_JSON)
                .principal(PrincipalMockFactory.mockPrincipal("nakadi")))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.content().json(
                        TestUtils.OBJECT_MAPPER.writeValueAsString(timelineViews)));
    }

}
