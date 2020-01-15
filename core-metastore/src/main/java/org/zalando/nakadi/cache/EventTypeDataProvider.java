package org.zalando.nakadi.cache;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.repository.db.EventTypeDbRepository;
import org.zalando.nakadi.util.JsonUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class EventTypeDataProvider implements CacheDataProvider<EventTypeDataProvider.VersionedEventType, String> {

    private final ObjectMapper objectMapper;
    private final EventTypeDbRepository eventTypeDbRepository;

    @Autowired
    public EventTypeDataProvider(
            final ObjectMapper objectMapper,
            final EventTypeDbRepository eventTypeDbRepository) {
        this.objectMapper = objectMapper;
        this.eventTypeDbRepository = eventTypeDbRepository;
    }

    @Override
    public VersionedEventType load(final String key) {
        return eventTypeDbRepository.findByNameO(key).map(this::convert).orElse(null);
    }

    @Override
    public CacheChange getFullChangeList(final Collection<VersionedEventType> snapshot) {
        final Map<String, String> currentValues = snapshot.stream()
                .collect(Collectors.toMap(VersionedEventType::getKey, VersionedEventType::getVersion));

        final List<EventTypeDbRepository.EtChange> changeset = eventTypeDbRepository.getChangeset(currentValues);
        return new CacheChange(
                changeset.stream()
                        .filter(v -> !v.isDeleted())
                        .map(EventTypeDbRepository.EtChange::getName)
                        .collect(Collectors.toList()),
                changeset.stream()
                        .filter(EventTypeDbRepository.EtChange::isDeleted)
                        .map(EventTypeDbRepository.EtChange::getName)
                        .collect(Collectors.toList())
        );
    }

    private VersionedEventType convert(final EventType et) {
        return new VersionedEventType(
                et,
                JsonUtils.serializeDateTime(objectMapper, et.getUpdatedAt())
        );
    }

    public static class VersionedEventType implements VersionedEntity<String> {

        private final EventType eventType;
        private final String updatedAt;

        private VersionedEventType(final EventType eventType, final String updatedAt) {
            this.eventType = eventType;
            this.updatedAt = updatedAt;
        }

        @Override
        public String getKey() {
            return eventType.getName();
        }

        @Override
        public String getVersion() {
            return updatedAt;
        }

        public EventType getEventType() {
            return eventType;
        }
    }
}
