package org.zalando.nakadi.repository.db;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.util.JsonUtils;
import org.zalando.nakadi.utils.TestUtils;

import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;


public class EventTypeDbRepositoryChangelistTest extends AbstractDbRepositoryTest {
    private EventTypeRepository repository;

    private final Map<String, String> existentEventTypes = new HashMap<>();

    @Before
    public void setUp() throws Exception {
        super.setUp();
        repository = new EventTypeRepository(template, TestUtils.OBJECT_MAPPER);
        existentEventTypes.clear();

        for (final EventType eventType : repository.list()) {
            existentEventTypes.put(
                    eventType.getName(), JsonUtils.serializeDateTime(repository.jsonMapper, eventType.getUpdatedAt()));
        }

        Stream.of("test1", "test2").forEach(etName -> {
            if (existentEventTypes.containsKey(etName)) {
                return;
            }
            final EventType eventType = buildDefaultEventType();
            eventType.setName(etName);
            final EventType created = repository.saveEventType(eventType);
            existentEventTypes.put(
                    created.getName(),
                    JsonUtils.serializeDateTime(repository.jsonMapper, eventType.getUpdatedAt()));
        });
    }

    @After
    public void tearDown() throws SQLException {
        super.tearDown();
    }


    @Test
    public void eventTypeAdded() {
        final Map<String, String> old = new HashMap<>();
        old.put("test1", existentEventTypes.get("test1"));

        final List<EventTypeRepository.EtChange> changeset = repository.getChangeset(old);

        // In this test it is expected that there are no changes to internal list in case if something was added
        Assert.assertEquals(Collections.emptyList(), changeset);
    }

    @Test
    public void eventTypeDeleted() {
        final Map<String, String> old = new HashMap<>();
        old.put("test1", existentEventTypes.get("test1"));
        old.put("non-existent-event-type", "it doesnt matter");

        final List<EventTypeRepository.EtChange> changeset = repository.getChangeset(old);

        Assert.assertEquals(
                Collections.singletonList(new EventTypeRepository.EtChange("non-existent-event-type", true)),
                changeset);
    }

    @Test
    public void eventTypeUpdated() {
        final Map<String, String> old = new HashMap<>();
        old.put("test1", existentEventTypes.get("test1"));
        old.put("test2", "something different");

        final List<EventTypeRepository.EtChange> changeset = repository.getChangeset(old);

        Assert.assertEquals(
                Collections.singletonList(new EventTypeRepository.EtChange("test2", false)),
                changeset);
    }
}
