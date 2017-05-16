package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.ForbiddenAccessException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoTopicFoundException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.security.Client;

import java.util.Optional;

@Component
public class TopicsService {

    private final SecuritySettings securitySettings;
    private final TimelineDbRepository timelineDbRepository;
    private final EventTypeRepository eventTypeRepository;

    @Autowired
    public TopicsService(final SecuritySettings securitySettings,
                         final TimelineDbRepository timelineDbRepository,
                         final EventTypeRepository eventTypeRepository) {
        this.securitySettings = securitySettings;
        this.timelineDbRepository = timelineDbRepository;
        this.eventTypeRepository = eventTypeRepository;
    }

    public EventType getEventTypeByTopic(final String topic, final Client client) {
        if (!client.getClientId().equals(securitySettings.getAdminClientId())) {
            throw new ForbiddenAccessException("Request is forbidden for user " + client.getClientId());
        }

        final Optional<String> etName = timelineDbRepository.listTimelinesOrdered().stream()
                .filter(tl -> tl.isActive())
                .filter(tl -> tl.getTopic().equals(topic))
                .findFirst()
                .map(Timeline::getEventType);

        if (etName.isPresent()) {
            return etName.map(etn -> {
                try {
                    return eventTypeRepository.findByName(etn);
                } catch (final NakadiException e) {
                    throw new NakadiRuntimeException(e);
                }
            }).get();
        }

        return eventTypeRepository.list().stream()
                .filter(et -> et.getTopic().equals(topic))
                .findFirst()
                .orElseThrow(() -> new NoTopicFoundException("No topic found: " + topic));

    }
}
