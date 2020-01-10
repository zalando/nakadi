package org.zalando.nakadi.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.exceptions.runtime.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Component
public class  SystemEventTypeInitializer {
    private final ObjectMapper objectMapper;
    private final EventTypeService eventTypeService;
    private static final Logger LOG = LoggerFactory.getLogger(SystemEventTypeInitializer.class);

    @Autowired
    public SystemEventTypeInitializer(
            final ObjectMapper objectMapper,
            final EventTypeService eventTypeService) {
        this.objectMapper = objectMapper;
        this.eventTypeService = eventTypeService;
    }

    public void createEventTypesFromResource(
            final String resourceName,
            final Map<String, String> nameReplacements) throws IOException {
        LOG.debug("Initializing event types from {}", resourceName);
        String eventTypesString = Resources.toString(Resources.getResource(resourceName), Charsets.UTF_8);
        for (final Map.Entry<String, String> entry : nameReplacements.entrySet()) {
            eventTypesString = eventTypesString.replaceAll(
                    Pattern.quote(entry.getKey()),
                    Matcher.quoteReplacement(entry.getValue()));
        }

        final TypeReference<List<EventTypeBase>> typeReference = new TypeReference<List<EventTypeBase>>() {
        };
        final List<EventTypeBase> eventTypes = objectMapper.readValue(eventTypesString, typeReference);

        eventTypes.forEach(et -> {
            try {
                eventTypeService.create(et, false);
            } catch (final DuplicatedEventTypeNameException e) {
                LOG.debug("Event type {} from {} already exists", et.getName(), resourceName);
            } catch (final NakadiBaseException e) {
                LOG.debug("Problem creating event type {} from {}", et.getName(), resourceName, e);
            }
        });

    }
}
