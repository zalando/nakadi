package org.zalando.nakadi.metrics;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypeOptions;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.Permission;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;
import org.zalando.nakadi.exceptions.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.EventTypeTimeoutException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.partitioning.PartitionStrategy;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.repository.db.AuthorizationDbRepository;
import org.zalando.nakadi.service.EventPublisher;
import org.zalando.nakadi.service.EventTypeService;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Component
public class KpiMetricsDumper {
    private static final long DUMP_INTERVAL_MS = 1000;
    private final NakadiKPIMetrics collector;
    private final EventPublisher eventPublisher;
    private final EventTypeService etService;
    private final AuthorizationDbRepository authorizationDbRepository;
    private static final Logger LOG = LoggerFactory.getLogger(KpiMetricsDumper.class);

    public KpiMetricsDumper(
            final NakadiKPIMetrics collector,
            final EventPublisher eventPublisher,
            final EventTypeService etService,
            final AuthorizationDbRepository authorizationDbRepository) {
        this.collector = collector;
        this.eventPublisher = eventPublisher;
        this.etService = etService;
        this.authorizationDbRepository = authorizationDbRepository;
    }

    @Scheduled(fixedDelay = DUMP_INTERVAL_MS)
    public void periodicDump() {
        final Map<String, List<MeasurementEvent>> groupedByEts = new HashMap<>();
        List<MetricsCollectorImpl> items;
        while (!(items = collector.poll(NakadiKPIMetrics.EVENT_PACK_SIZE)).isEmpty()) {
            items.forEach(item ->
                    groupedByEts.computeIfAbsent(item.getEt(), k -> new ArrayList<>()).add(toEvent(item)));

            for (final Map.Entry<String, List<MeasurementEvent>> entry : groupedByEts.entrySet()) {
                if (!entry.getValue().isEmpty()) {
                    dumpToStorage(entry.getKey(), entry.getValue());
                }
            }
            groupedByEts.clear();
            if (items.size() < NakadiKPIMetrics.EVENT_PACK_SIZE) {
                break;
            }
        }
    }

    private MeasurementEvent toEvent(final MetricsCollectorImpl collector) {
        final MeasurementEvent event = new MeasurementEvent();
        event.setStartedAt(collector.getStart());
        event.setEndedAt(collector.getFinish());
        event.setDurationMillis(collector.getFinish() - collector.getStart());
        event.setAdditional(collector.getAdditional());
        final MeasurementEvent.MeasurementStep[] steps =
                new MeasurementEvent.MeasurementStep[collector.getSteps().size()];
        for (int i = 0; i < steps.length; ++i) {
            final MeasurementEvent.MeasurementStep mStep = new MeasurementEvent.MeasurementStep();
            final StepImpl cStep = collector.getSteps().get(i);
            mStep.setCount(cStep.getCount());
            mStep.setName(cStep.getName());
            mStep.setTimeTotalMs(TimeUnit.NANOSECONDS.toMillis(cStep.getOverallDurationNs()));
            steps[i] = mStep;
        }
        event.setSteps(steps);
        return event;
    }

    private void dumpToStorage(final String et, final List<MeasurementEvent> group) {
        final String events = "[" +
                group.stream()
                        .map(JSONObject::new)
                        .map(JSONObject::toString)
                        .collect(Collectors.joining(","))
                + "]";
        try {
            eventPublisher.publish(events, et);
        } catch (NoSuchEventTypeException e) {
            if (!registerEventType(et)) {
                LOG.error("Event type {} registration failed. Skipping {} events", et, group.size());
                return;
            }
            try {
                eventPublisher.publish(events, et);
            } catch (final NoSuchEventTypeException | InternalNakadiException | EventTypeTimeoutException ex) {
                LOG.error("Failed to publish {} metrics events to event type {}, et created", group.size(), et, e);
            }

        } catch (final InternalNakadiException | EventTypeTimeoutException e) {
            LOG.error("Failed to publish {} metrics events to event type {}", group.size(), et, e);
        }

    }

    private ResourceAuthorization createAuthorization() {
        final List<AuthorizationAttribute> read = new ArrayList<>();
        final List<AuthorizationAttribute> write = new ArrayList<>();
        final List<AuthorizationAttribute> admin = new ArrayList<>();
        for (final Permission permission : authorizationDbRepository.listAdmins()) {
            switch (permission.getOperation()) {
                case READ:
                    read.add(permission.getAuthorizationAttribute());
                    break;
                case WRITE:
                    write.add(permission.getAuthorizationAttribute());
                    break;
                case ADMIN:
                    admin.add(permission.getAuthorizationAttribute());
                    break;
                default:
                    LOG.warn("Operation {} is ignored while creating metric event type", permission.getOperation());
            }
        }
        // TODO: Use more sophisticated approach
        write.add(new ResourceAuthorizationAttribute("*", "*"));
        return new ResourceAuthorization(admin, read, write);
    }


    private boolean registerEventType(final String et) {
        final EventTypeBase eventType = new EventTypeBase();
        eventType.setAuthorization(createAuthorization());
        eventType.setCategory(EventCategory.UNDEFINED);
        eventType.setCompatibilityMode(CompatibilityMode.NONE);
        eventType.setDefaultStatistic(new EventTypeStatistics(8, 8));
        eventType.setName(et);
        eventType.setOptions(new EventTypeOptions());
        eventType.setOwningApplication("nakadi");
        eventType.setPartitionStrategy(PartitionStrategy.RANDOM_STRATEGY);
        eventType.setSchema(
                new EventTypeSchemaBase(EventTypeSchemaBase.Type.JSON_SCHEMA, "{\"additionalProperties\": true}"));
        try {
            etService.create(eventType);
        } catch (DuplicatedEventTypeNameException ignore) {
        } catch (Exception ex) {
            LOG.error("Failed to create event type for event {}", et, ex);
            return false;
        }
        return true;
    }

}
