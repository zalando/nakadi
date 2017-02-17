package org.zalando.nakadi.service.subscription;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.service.timeline.TimelineService;

@Component
public class SubscriptionKafkaClientFactory {

    private final TimelineService timelineService;
    private final EventTypeRepository eventTypeRepository;

    @Autowired
    public SubscriptionKafkaClientFactory(final TimelineService timelineService,
                                          final EventTypeRepository eventTypeRepository) {
        this.timelineService = timelineService;
        this.eventTypeRepository = eventTypeRepository;
    }

    public KafkaClient createKafkaClient(final Subscription subscription) {
        return new KafkaClient(subscription, timelineService, eventTypeRepository);
    }
}
