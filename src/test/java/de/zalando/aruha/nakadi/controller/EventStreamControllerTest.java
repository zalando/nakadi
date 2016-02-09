package de.zalando.aruha.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.config.NakadiConfig;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.service.EventStream;
import de.zalando.aruha.nakadi.service.EventStreamConfig;
import de.zalando.aruha.nakadi.service.EventStreamFactory;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

public class EventStreamControllerTest {

    private static final String TEST_EVENT_TYPE = "test";

    private final ObjectMapper objectMapper = new NakadiConfig().jacksonObjectMapper();

    private final MappingJackson2HttpMessageConverter messageConverter =
            new MappingJackson2HttpMessageConverter(objectMapper);

    private MockMvc mockMvc;

    private TopicRepository topicRepositoryMock;

    private EventStreamFactory eventStreamFactoryMock;

    @Before
    public void setup() {
        topicRepositoryMock = mock(TopicRepository.class);
        eventStreamFactoryMock = mock(EventStreamFactory.class);

        final EventStreamController controller = new EventStreamController(topicRepositoryMock, objectMapper,
                eventStreamFactoryMock);

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), messageConverter)
                .build();
    }

    @Test
    public void whenNoParamsThenDefaultsAreUsed() throws Exception {
        final ArgumentCaptor<EventStreamConfig> configCaptor = ArgumentCaptor.forClass(EventStreamConfig.class);
        final EventStream eventStreamMock = mock(EventStream.class);
        when(eventStreamFactoryMock.createEventStream(any(), any(), configCaptor.capture()))
                .thenReturn(eventStreamMock);

        when(topicRepositoryMock.topicExists(eq(TEST_EVENT_TYPE))).thenReturn(true);
        when(topicRepositoryMock.areCursorsValid(eq(TEST_EVENT_TYPE), any())).thenReturn(true);

        mockMvc.perform(
                get(createUrl(TEST_EVENT_TYPE))
                        .header("X-nakadi-cursors", "[{\"partition\":\"0\",\"offset\":\"0\"}]"))
                .andExpect(status().isOk());

        // we have to sleep here as mockMvc exits at the very beginning, before the body starts streaming
        Thread.sleep(2000);

        final EventStreamConfig expectedConfig = EventStreamConfig
                .builder()
                .withTopic(TEST_EVENT_TYPE)
                .withBatchLimit(1)
                .withBatchTimeout(30)
                .withCursors(ImmutableMap.of("0", "0"))
                .withStreamKeepAliveLimit(0)
                .withStreamLimit(0)
                .withStreamTimeout(0)
                .build();
        assertThat(configCaptor.getValue(), equalTo(expectedConfig));
    }

    private String createUrl(final String eventType) {
        return String.format("/event-types/%s/events", eventType);
    }
}
