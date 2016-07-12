package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.enrichment.Enrichment;
import de.zalando.aruha.nakadi.managers.EventTypeManager;
import de.zalando.aruha.nakadi.partitioning.PartitionResolver;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.util.UUIDGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ManagersConfig {

    @Bean
    public EventTypeManager eventTypeManager(final EventTypeRepository eventTypeRepository,
                                             final TopicRepository topicRepository,
                                             final PartitionResolver partitionResolver,
                                             final Enrichment enrichment,
                                             final UUIDGenerator uuidGenerator)
    {
        return new EventTypeManager(eventTypeRepository, topicRepository, partitionResolver, enrichment, uuidGenerator);
    }
}
