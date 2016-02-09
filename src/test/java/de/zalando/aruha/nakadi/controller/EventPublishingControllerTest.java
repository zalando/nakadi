package de.zalando.aruha.nakadi.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.config.NakadiConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.InMemoryEventTypeRepository;
import de.zalando.aruha.nakadi.repository.InMemoryTopicRepository;
import org.junit.Test;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;
import uk.co.datumedge.hamcrest.json.SameJSONAs;

import javax.ws.rs.core.Response;
import java.util.LinkedList;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class EventPublishingControllerTest {

    public static final String EVENT_TYPE_WITH_TOPIC = "my-topic";
    public static final String EVENT_TYPE_WITHOUT_TOPIC = "registered-but-without-topic";
    public static final String EVENT1 = "{\"payload\": \"My Event 1 Payload\"}";
    public static final String EVENT2 = "{\"payload\": \"My Event 2 Payload\"}";
    public static final String EVENT3 = "{\"payload\": \"My Event 3 Payload\"}";

    private final InMemoryTopicRepository topicRepository = new InMemoryTopicRepository();
    private final EventTypeRepository eventTypeRepository = new InMemoryEventTypeRepository();
    private final ObjectMapper objectMapper = new NakadiConfig().jacksonObjectMapper();

    private final MockMvc mockMvc;

    public EventPublishingControllerTest() throws NakadiException, DuplicatedEventTypeNameException {
        topicRepository.createTopic(EVENT_TYPE_WITH_TOPIC);

        eventTypeRepository.saveEventType(eventType(EVENT_TYPE_WITH_TOPIC));
        eventTypeRepository.saveEventType(eventType(EVENT_TYPE_WITHOUT_TOPIC));

        EventPublishingController controller = new EventPublishingController(topicRepository, eventTypeRepository);

        final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter = new MappingJackson2HttpMessageConverter(objectMapper);
        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), jackson2HttpMessageConverter)
                .build();
    }

    @Test
    public void canPostEventsToTopic() throws Exception {
        postEvent(EVENT_TYPE_WITH_TOPIC, EVENT1);
        postEvent(EVENT_TYPE_WITH_TOPIC, EVENT2);
        postEvent(EVENT_TYPE_WITH_TOPIC, EVENT3);

        final LinkedList<String> events = topicRepository.getEvents(EVENT_TYPE_WITH_TOPIC, "1");

        assertThat(events, hasSize(3));

        assertThat(events.removeFirst(), equalTo(EVENT1));
        assertThat(events.removeFirst(), equalTo(EVENT2));
        assertThat(events.removeFirst(), equalTo(EVENT3));
    }

    @Test
    public void returns2xxForValidPost() throws Exception {
        postEvent(EVENT_TYPE_WITH_TOPIC, EVENT1).andExpect(status().is2xxSuccessful());
    }

    @Test
    public void returns5xxProblemIfTopicDoesNotExistForEventType() throws Exception  {
        final ThrowableProblem expectedProblem = Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR);

        postEvent(EVENT_TYPE_WITHOUT_TOPIC, EVENT1)
                .andExpect(status().is5xxServerError())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void returns404ProblemIfEventTypeIsNotRegistered() throws Exception  {
        final ThrowableProblem expectedProblem = Problem.valueOf(Response.Status.NOT_FOUND, "EventType 'does-not-exist' does not exist.");

        postEvent("does-not-exist", EVENT1)
                .andExpect(status().is4xxClientError())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    private ResultActions postEvent(final String eventType, final String event) throws Exception {
        final String url = "/event-types/" + eventType + "/events";
        final MockHttpServletRequestBuilder requestBuilder = post(url)
                .contentType(APPLICATION_JSON)
                .content(event);

        return mockMvc.perform(requestBuilder);
    }

    private static EventType eventType(final String topic) {
        final EventType eventType = new EventType();
        eventType.setName(topic);
        return eventType;
    }

    private SameJSONAs<? super String> matchesProblem(final ThrowableProblem expectedProblem) throws JsonProcessingException {
        return sameJSONAs(asJsonString(expectedProblem));
    }

    private String asJsonString(final ThrowableProblem expectedProblem) throws JsonProcessingException {
        return objectMapper.writeValueAsString(expectedProblem);
    }
}