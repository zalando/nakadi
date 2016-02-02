package de.zalando.aruha.nakadi.controller;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.InMemoryEventTypeRepository;
import de.zalando.aruha.nakadi.repository.InMemoryTopicRepository;
import org.junit.Test;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.LinkedList;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class EventSubmissionControllerTest {

    public static final String EVENT_TYPE_WITH_TOPIC = "my-topic";
    public static final String EVENT_TYPE_WITHOUT_TOPIC = "registered-but-without-topic";
    public static final String EVENT1 = "My Event 1 Payload";
    public static final String EVENT2 = "My Event 2 Payload";
    public static final String EVENT3 = "My Event 3 Payload";

    private final InMemoryTopicRepository topicRepository = new InMemoryTopicRepository();
    private final EventTypeRepository eventTypeRepository = new InMemoryEventTypeRepository();
    private final EventPublishingController controller = new EventPublishingController(topicRepository, eventTypeRepository);
    private MockMvc mockMvc = MockMvcBuilders.standaloneSetup(controller).build();

    public EventSubmissionControllerTest() throws NakadiException {
        topicRepository.createTopic(EVENT_TYPE_WITH_TOPIC, 3);

        eventTypeRepository.saveEventType(eventType(EVENT_TYPE_WITH_TOPIC));
        eventTypeRepository.saveEventType(eventType(EVENT_TYPE_WITHOUT_TOPIC));
    }

    @Test
    public void canPostEventsToTopic() throws Exception {

        controller.postEvent(EVENT_TYPE_WITH_TOPIC, EVENT1);
        controller.postEvent(EVENT_TYPE_WITH_TOPIC, EVENT2);
        controller.postEvent(EVENT_TYPE_WITH_TOPIC, EVENT3);

        final LinkedList<String> events = topicRepository.getEvents(EVENT_TYPE_WITH_TOPIC, "1");

        assertThat(events, hasSize(3));

        assertThat(events.removeFirst(), equalTo(EVENT1));
        assertThat(events.removeFirst(), equalTo(EVENT2));
        assertThat(events.removeFirst(), equalTo(EVENT3));
    }

    @Test
    public void returns2xxForValidPost() throws Exception {
        final String url = "/event-types/" + EVENT_TYPE_WITH_TOPIC + "/events";
        final MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post(url);
        requestBuilder.content(EVENT1);

        mockMvc.perform(requestBuilder).andExpect(status().is2xxSuccessful());
    }

    @Test
    public void returns5xxIfTopicDoesNotExistForEventType() throws Exception  {
        final String url = "/event-types/" + EVENT_TYPE_WITHOUT_TOPIC + "/events";
        final MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post(url);
        requestBuilder.content(EVENT1);

        mockMvc.perform(requestBuilder).andExpect(status().is5xxServerError());
    }

    @Test
    public void returns404IfEventTypeIsNotRegistered() throws Exception  {
        final String url = "/event-types/does-not-exist/events";
        final MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post(url);
        requestBuilder.content(EVENT1);

        mockMvc.perform(requestBuilder).andExpect(status().is4xxClientError());
    }

    private static EventType eventType(final String topic) {
        final EventType eventType = new EventType();
        eventType.setName(topic);
        return eventType;
    }

}