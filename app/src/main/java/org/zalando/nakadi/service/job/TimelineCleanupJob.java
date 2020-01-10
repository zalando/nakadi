package org.zalando.nakadi.service.job;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.TopicDeletionException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.ThreadUtils;

import java.util.List;

@Service
public class TimelineCleanupJob {

    private static final String JOB_NAME = "timelines-cleanup";

    private static final Logger LOG = LoggerFactory.getLogger(TimelineCleanupJob.class);

    private final EventTypeCache eventTypeCache;
    private final TimelineDbRepository timelineDbRepository;
    private final TimelineService timelineService;
    private final FeatureToggleService featureToggleService;
    private final ExclusiveJobWrapper jobWrapper;
    private final long deletionDelayMs;

    @Autowired
    public TimelineCleanupJob(final EventTypeCache eventTypeCache,
                              final TimelineDbRepository timelineDbRepository,
                              final TimelineService timelineService,
                              final FeatureToggleService featureToggleService,
                              final JobWrapperFactory jobWrapperFactory,
                              @Value("${nakadi.jobs.timelineCleanup.runPeriodMs}") final int periodMs,
                              @Value("${nakadi.jobs.timelineCleanup.deletionDelayMs}") final long deletionDelayMs) {
        this.eventTypeCache = eventTypeCache;
        this.timelineDbRepository = timelineDbRepository;
        this.timelineService = timelineService;
        this.jobWrapper = jobWrapperFactory.createExclusiveJobWrapper(JOB_NAME, periodMs);
        this.featureToggleService = featureToggleService;
        this.deletionDelayMs = deletionDelayMs;
    }

    @Scheduled(
            fixedDelayString = "${nakadi.jobs.checkRunMs}",
            initialDelayString = "${random.int(${nakadi.jobs.checkRunMs})}")
    public void cleanupTimelines() {
        if (featureToggleService.isFeatureEnabled(Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            LOG.warn("Skipping timelines cleanup because write operations to the DB are disabled");
            return;
        }
        try {
            jobWrapper.runJobLocked(this::deleteTimelinesLocked);
        } catch (final RepositoryProblemException e) {
            LOG.error("DB error occurred when trying to get expired timelines", e);
        }
    }

    private void deleteTimelinesLocked() {
        final List<Timeline> expired = timelineDbRepository.getExpiredTimelines();
        for (int i = 0; i < expired.size(); ++i) {
            if (i != 0 && deletionDelayMs > 0) {
                try {
                    ThreadUtils.sleep(deletionDelayMs);
                } catch (InterruptedException e) {
                    LOG.warn("Timeline deletion thread was interrupted", e);
                    Thread.currentThread().interrupt();
                    return;
                }
            }
            final Timeline timeline = expired.get(i);
            deleteTimelineTopic(timeline);
            markTimelineDeleted(timeline);
        }
    }

    private void deleteTimelineTopic(final Timeline timeline) {
        try {
            final TopicRepository topicRepository = timelineService.getTopicRepository(timeline);
            topicRepository.deleteTopic(timeline.getTopic());
        } catch (final TopicDeletionException e) {
            LOG.error("Failed to delete topic {} for expired timeline {}", timeline.getTopic(), timeline.getId(), e);
        }
    }

    private void markTimelineDeleted(final Timeline timeline) {
        boolean timelineUpdatedInDB = false;
        boolean cacheUpdated = false;
        try {
            timeline.setDeleted(true);
            timelineDbRepository.updateTimelime(timeline);
            timelineUpdatedInDB = true;

            eventTypeCache.updated(timeline.getEventType());
            cacheUpdated = true;
        } catch (final InconsistentStateException e) {
            LOG.error("Failed to serialize timeline to DB when marking timeline as deleted", e);
        } catch (final RepositoryProblemException e) {
            LOG.error("DB failure when marking timeline as deleted", e);
        } catch (Exception e) {
            LOG.error("ZK error occurred when updating ET cache", e);
        } finally {
            // revert timeline state in a case if cache wasn't updated successfully
            if (timelineUpdatedInDB && !cacheUpdated) {
                try {
                    timeline.setDeleted(false);
                    timelineDbRepository.updateTimelime(timeline);
                } catch (final Exception e) {
                    LOG.error("Failed to revert timeline state", e);
                }
            }
        }
    }

}
