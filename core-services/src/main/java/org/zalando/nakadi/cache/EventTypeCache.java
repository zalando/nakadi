package org.zalando.nakadi.cache;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.validation.EventTypeValidator;
import org.zalando.nakadi.validation.EventValidation;

import javax.annotation.Nonnull;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

@Service
public class EventTypeCache {

    // Enforce cache refresh/listener check every 5 minutes
    private static final long FORCE_REFRESH_MS = TimeUnit.MINUTES.toMillis(5);
    private static final String CACHE_ZK_NODE = "/nakadi/event_types_cache";

    private final Cache<CachedValue> cache;
    private final TimelineDbRepository timelineRepository;
    private final TimelineSync timelineSync;
    private final ZookeeperNodeInvalidator nodeInvalidator;
    private final Map<String, TimelineSync.ListenerRegistration> timelineRegistrations = new ConcurrentHashMap<>();

    @Autowired
    public EventTypeCache(
            final ZooKeeperHolder zooKeeperHolder,
            final EventTypeDataProvider eventTypeDataProvider,
            final TimelineDbRepository timelineRepository,
            final TimelineSync timelineSync) {
        cache = new SimpleCache<>(
                eventTypeDataProvider,
                this::convertAndRegister
        );
        nodeInvalidator = new ZookeeperNodeInvalidator(
                cache,
                zooKeeperHolder,
                CACHE_ZK_NODE,
                FORCE_REFRESH_MS);

        this.timelineRepository = timelineRepository;
        this.timelineSync = timelineSync;
    }

    @PostConstruct
    public void start() {
        this.nodeInvalidator.start();
    }

    @PreDestroy
    public void stop() {
        this.nodeInvalidator.stop();
        this.timelineRegistrations.forEach((s, listenerRegistration) -> listenerRegistration.cancel());
        this.timelineRegistrations.clear();
    }

    private CachedValue convertAndRegister(final EventTypeDataProvider.VersionedEventType versionedEventType) {
        final List<Timeline> timelines =
                timelineRepository.listTimelinesOrdered(versionedEventType.getEventType().getName());

        timelineRegistrations.computeIfAbsent(versionedEventType.getKey(), n ->
                timelineSync.registerTimelineChangeListener(n, cache::invalidate));

        return new CachedValue(
                versionedEventType.getEventType(),
                EventValidation.forType(versionedEventType.getEventType()),
                timelines
        );
    }

    public void updated(final String name) {
        cache.invalidate(name);
        created(name);
    }

    public void created(final String name) {
        nodeInvalidator.notifyUpdate();
        timelineRegistrations.computeIfAbsent(name,
                n -> timelineSync.registerTimelineChangeListener(n, cache::invalidate));
    }

    public void removed(final String name) {
        cache.invalidate(name);
        Optional.ofNullable(timelineRegistrations.remove(name))
                .ifPresent(TimelineSync.ListenerRegistration::cancel);
        nodeInvalidator.notifyUpdate();
    }

    public EventType getEventType(final String name) throws NoSuchEventTypeException {
        return getCached(name).getEventType();
    }

    public EventTypeValidator getValidator(final String name) throws NoSuchEventTypeException {
        return getCached(name).getEventTypeValidator();
    }

    public List<Timeline> getTimelinesOrdered(final String name) throws NoSuchEventTypeException {
        return getCached(name).getTimelines();
    }

    private CachedValue getCached(final String name) {
        final CachedValue value = cache.get(name);
        if (null == value) {
            throw new NoSuchEventTypeException("EventType \"" + name + "\" does not exist.");
        }
        return value;
    }

    public void addInvalidationListener(final Consumer<String> onEventTypeInvalidated) {
        cache.addInvalidationListener(onEventTypeInvalidated);
    }

    private static class CachedValue {
        private final EventType eventType;
        private final EventTypeValidator eventTypeValidator;
        @Nonnull
        private final List<Timeline> timelines;

        CachedValue(final EventType eventType,
                    final EventTypeValidator eventTypeValidator,
                    final List<Timeline> timelines) {
            this.eventType = eventType;
            this.eventTypeValidator = eventTypeValidator;
            this.timelines = timelines;
        }

        public EventType getEventType() {
            return eventType;
        }

        public EventTypeValidator getEventTypeValidator() {
            return eventTypeValidator;
        }

        public List<Timeline> getTimelines() {
            return timelines;
        }
    }

    public Optional<EventType> getEventTypeO(final String eventTypeName) throws InternalNakadiException {
        try {
            return Optional.of(getEventType(eventTypeName));
        } catch (final NoSuchEventTypeException e) {
            return Optional.empty();
        } catch (final InternalNakadiException e) {
            throw e;
        }
    }
}
