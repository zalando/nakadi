package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.enrichment.Enrichment;
import de.zalando.aruha.nakadi.service.EventTypeService;
import de.zalando.aruha.nakadi.partitioning.PartitionResolver;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.util.UUIDGenerator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ServiceConfig {

    @Bean
    public EventTypeService eventTypeService(final EventTypeRepository eventTypeRepository,
                                             final TopicRepository topicRepository,
                                             final PartitionResolver partitionResolver,
                                             final Enrichment enrichment,
                                             final UUIDGenerator uuidGenerator)
    {
        return new EventTypeService(eventTypeRepository, topicRepository, partitionResolver, enrichment, uuidGenerator);
    }
}
