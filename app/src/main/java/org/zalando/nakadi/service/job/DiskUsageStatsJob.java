package org.zalando.nakadi.service.job;

import com.google.common.annotations.VisibleForTesting;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.service.SystemEventTypeInitializer;
import org.zalando.nakadi.service.publishing.EventMetadata;
import org.zalando.nakadi.service.publishing.EventsProcessor;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class DiskUsageStatsJob {
    private static final String JOB_NAME = "disk-usage";

    private final ExclusiveJobWrapper wrapper;
    private final TimelineDbRepository timelineDbRepository;
    private final TopicRepositoryHolder topicRepositoryHolder;
    private final EventsProcessor eventsProcessor;
    private final SystemEventTypeInitializer systemEventTypeInitializer;
    private final DiskUsageStatsConfig config;
    private final EventMetadata eventMetadata;

    private static final Logger LOG = LoggerFactory.getLogger(DiskUsageStatsJob.class);

    public DiskUsageStatsJob(
            final JobWrapperFactory jobWrapperFactory,
            final TimelineDbRepository timelineDbRepository,
            final TopicRepositoryHolder topicRepositoryHolder,
            final EventsProcessor eventsProcessor,
            final SystemEventTypeInitializer systemEventTypeInitializer,
            final DiskUsageStatsConfig config,
            final EventMetadata eventMetadata) {
        this.timelineDbRepository = timelineDbRepository;
        this.topicRepositoryHolder = topicRepositoryHolder;
        this.eventsProcessor = eventsProcessor;
        this.systemEventTypeInitializer = systemEventTypeInitializer;
        this.config = config;
        this.wrapper = jobWrapperFactory.createExclusiveJobWrapper(JOB_NAME, config.getRunPeriodMs());
        this.eventMetadata = eventMetadata;
    }

    @PostConstruct
    public void prepareEventType() throws IOException {
        final Map<String, String> nameReplacements = new HashMap<>();
        nameReplacements.put("event_type_name_placeholder", config.getEventTypeName());
        nameReplacements.put("owning_application_placeholder", config.getOwningApplication());
        nameReplacements.put("auth_data_type_placeholder", config.getAuthDataType());
        nameReplacements.put("auth_value_placeholder", config.getAuthValue());
        this.systemEventTypeInitializer.createEventTypesFromResource(
                "disk_usage_event_type.json",
                nameReplacements);
    }

    @Scheduled(
            fixedDelayString = "${nakadi.jobs.checkRunMs}",
            initialDelayString = "${random.int(${nakadi.jobs.checkRunMs})}")
    public void dumpDiskStats() {
        try {
            wrapper.runJobLocked(this::dumpDiskStatsLocked);
        } catch (RuntimeException ex) {
            LOG.warn("Disk stats dump failed", ex);
        }
    }

    private void dumpDiskStatsLocked() {
        final Map<String, Long> eventTypeSize = loadDiskUsage();
        publishSizeStats(eventTypeSize);
    }

    @VisibleForTesting
    Map<String, Long> loadDiskUsage() {
        final Map<String, Map<String, String>> storageTopicToEventType = new HashMap<>();
        final Map<String, Storage> storages = new HashMap<>();
        final List<Timeline> allTimelines = timelineDbRepository.listTimelinesOrdered();
        for (final Timeline t : allTimelines) {
            if (t.isDeleted()) {
                continue;
            }
            storages.put(t.getStorage().getId(), t.getStorage());
            storageTopicToEventType
                    .computeIfAbsent(t.getStorage().getId(), v -> new HashMap<>())
                    .put(t.getTopic(), t.getEventType());
        }

        final Map<String, Long> eventTypeSize = new HashMap<>();

        for (final Map.Entry<String, Map<String, String>> storageEntry : storageTopicToEventType.entrySet()) {
            final Map<TopicPartition, Long> data = topicRepositoryHolder
                    .getTopicRepository(storages.get(storageEntry.getKey()))
                    .getSizeStats();
            data.entrySet().stream()
                    .filter(v -> storageEntry.getValue().containsKey(v.getKey().getTopic()))
                    .forEach(item -> {
                        final String eventType = storageEntry.getValue().get(item.getKey().getTopic());
                        eventTypeSize.compute(
                                eventType,
                                (et, oldSize) -> null == oldSize ? item.getValue() : (oldSize + item.getValue()));
                    });
        }
        return eventTypeSize;
    }

    private void publishSizeStats(final Map<String, Long> eventTypeSizes) {
        eventTypeSizes.entrySet().stream()
                .map(x -> {
                    final JSONObject event = new JSONObject();
                    event.put("event_type", x.getKey());
                    event.put("size_bytes", x.getValue() * 1024);
                    return eventMetadata.addTo(event);
                })
                .forEach(item -> eventsProcessor.queueEvent(config.getEventTypeName(), item));
    }
}
