package org.zalando.nakadi.config;


import com.google.common.collect.Lists;
import org.apache.log4j.NDC;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.publishing.EventOwnerExtractorFactory;
import org.zalando.nakadi.service.publishing.check.Check;
import org.zalando.nakadi.service.publishing.check.EnrichmentCheck;
import org.zalando.nakadi.service.publishing.check.EventDeletionEmptyPayloadCheck;
import org.zalando.nakadi.service.publishing.check.EventKeyCheck;
import org.zalando.nakadi.service.publishing.check.EventOwnerSelectorCheck;
import org.zalando.nakadi.service.publishing.check.EventSizeCheck;
import org.zalando.nakadi.service.publishing.check.EventTypeCheck;
import org.zalando.nakadi.service.publishing.check.PartitioningCheck;

import java.util.List;
import java.util.concurrent.Executors;


@Configuration
@EnableScheduling
public class NakadiConfig {

    @Bean
    public AsyncTaskExecutor asyncTaskExecutor() {
        final ConcurrentTaskExecutor taskExecutor = new ConcurrentTaskExecutor(Executors.newCachedThreadPool());
        taskExecutor.setTaskDecorator(new TaskDecorator() {
            @Override
            public Runnable decorate(final Runnable runnable) {
                return new Runnable() {
                    @Override
                    public void run() {
                        NDC.clear();
                        MDC.clear();

                        runnable.run();
                    }
                };
            }
        });
        return taskExecutor;
    }

    @Bean
    @Qualifier("internal-publishing-checks")
    public List<Check> internalPublishingChecks(final EnrichmentCheck enrichmentCheck,
                                                final PartitioningCheck partitioningCheck) {
        return Lists.newArrayList(
                partitioningCheck,
                enrichmentCheck,
                new EventKeyCheck()
        );
    }

    @Bean
    @Qualifier("pre-publishing-checks")
    public List<Check> prePublishingChecks(final EventTypeCheck eventTypeCheck,
                                           final EventOwnerExtractorFactory eventOwnerExtractorFactory,
                                           final AuthorizationValidator authValidator,
                                           final EnrichmentCheck enrichmentCheck,
                                           final PartitioningCheck partitioningCheck) {
        return Lists.newArrayList(
                eventTypeCheck,
                new EventOwnerSelectorCheck(eventOwnerExtractorFactory, authValidator),
                new EventSizeCheck(),
                partitioningCheck,
                enrichmentCheck,
                new EventKeyCheck()
        );
    }

    @Bean
    @Qualifier("pre-deleting-checks")
    public List<Check> preDeletingChecks(final EventTypeCheck eventTypeCheck,
                                         final EventOwnerExtractorFactory eventOwnerExtractorFactory,
                                         final AuthorizationValidator authValidator,
                                         final PartitioningCheck partitioningCheck) {
        // TODO: potentially we could skip the event type check for deletion
        return Lists.newArrayList(
                eventTypeCheck,
                new EventOwnerSelectorCheck(eventOwnerExtractorFactory, authValidator),
                new EventDeletionEmptyPayloadCheck(),
                partitioningCheck,
                new EventKeyCheck()
        );
    }
}
