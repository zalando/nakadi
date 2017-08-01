package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.Optional;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.EventTypeDeletionException;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.kafka.PartitionsCalculator;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.FeatureToggleService;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import org.zalando.nakadi.validation.SchemaEvolutionService;

public class EventTypeServiceTest {

    private final Enrichment enrichment = mock(Enrichment.class);
    private final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
    private EventTypeService eventTypeService;
    private final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
    private final NakadiSettings nakadiSettings = mock(NakadiSettings.class);
    private final PartitionsCalculator partitionsCalculator = mock(PartitionsCalculator.class);
    private final PartitionResolver partitionResolver = mock(PartitionResolver.class);
    private final SchemaEvolutionService schemaEvolutionService = mock(SchemaEvolutionService.class);
    private final SubscriptionDbRepository subscriptionDbRepository = mock(SubscriptionDbRepository.class);
    private final TimelineService timelineService = mock(TimelineService.class);
    private final TimelineSync timelineSync = mock(TimelineSync.class);
    private final TransactionTemplate transactionTemplate = mock(TransactionTemplate.class);
    private final AuthorizationValidator authorizationValidator = mock(AuthorizationValidator.class);

    @Before
    public void setUp() {
        eventTypeService = new EventTypeService(eventTypeRepository, timelineService, partitionResolver, enrichment,
                subscriptionDbRepository, schemaEvolutionService, partitionsCalculator, featureToggleService,
                authorizationValidator, timelineSync, transactionTemplate, nakadiSettings);
        when(transactionTemplate.execute(any())).thenAnswer(invocation -> {
            final TransactionCallback callback = (TransactionCallback) invocation.getArguments()[0];
            return callback.doInTransaction(null);
        });

    }

    @Test
    public void testFailToDeleteEventType() throws Exception {
        final EventType eventType = buildDefaultEventType();
        doThrow(new InternalNakadiException("Can't delete event tye"))
                .when(eventTypeRepository).removeEventType(eventType.getName());
        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());
        final Multimap<TopicRepository, String> topicsToDelete = mock(Multimap.class);
        doReturn(new ArrayList<Subscription>())
                .when(subscriptionDbRepository)
                .listSubscriptions(ImmutableSet.of(eventType.getName()), Optional.empty(), 0, 1);
        doReturn(topicsToDelete).when(timelineService).deleteAllTimelinesForEventType(eventType.getName());
        try {
            eventTypeService.delete(eventType.getName());
        } catch (final EventTypeDeletionException e) {
            // check that topics are not deleted in Kafka
            verifyZeroInteractions(topicsToDelete);
            return;
        }

        fail("Should have thrown an EventTypeDeletionException");
    }
}
