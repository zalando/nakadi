package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.support.rowset.SqlRowSet;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.EventTypeRepository;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class EventTypeDbRepositoryTest extends AbstractDbRepositoryTest {

    private EventTypeRepository repository;

    public EventTypeDbRepositoryTest() {
        super("zn_data.event_type");
    }

    @Before
    public void setUp() {
        super.setUp();
        repository = new EventTypeDbRepository(template, mapper);
    }

    @Test
    public void whenCreateNewEventTypePersistItInTheDatabase() throws Exception {
        final EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.event_type", Integer.class);
        assertThat("Number of rows should increase", rows, equalTo(1));

        final SqlRowSet rs =
                template.queryForRowSet("SELECT et_name, et_topic, et_event_type_object FROM zn_data.event_type");
        rs.next();

        assertThat("Name is persisted", rs.getString(1), equalTo(eventType.getName()));
        assertThat("Topic is persisted", rs.getString(2), equalTo(eventType.getTopic()));

        final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();
        final EventType persisted = mapper.readValue(rs.getString(3), EventType.class);

        assertThat(persisted.getCategory(), equalTo(eventType.getCategory()));
        assertThat(persisted.getName(), equalTo(eventType.getName()));
        assertThat(persisted.getSchema().getType(), equalTo(eventType.getSchema().getType()));
        assertThat(persisted.getSchema().getSchema(), equalTo(eventType.getSchema().getSchema()));
    }

    @Test(expected = DuplicatedEventTypeNameException.class)
    public void whenCreateDuplicatedNamesThrowAnError() throws Exception {
        final EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);
        repository.saveEventType(eventType);
    }

    @Test
    public void whenEventExistsFindByNameReturnsSomething() throws Exception {
        final EventType eventType1 = buildDefaultEventType();
        final EventType eventType2 = buildDefaultEventType();

        insertEventType(eventType1);
        insertEventType(eventType2);

        final EventType persistedEventType = repository.findByName(eventType2.getName());

        assertThat(persistedEventType, notNullValue());
    }

    @Test(expected =  org.springframework.dao.DuplicateKeyException.class)
    public void validatesUniquenessOfTopic() throws Exception {
        final EventType eventType1 = buildDefaultEventType();
        final EventType eventType2 = buildDefaultEventType();
        eventType2.setTopic(eventType1.getTopic());

        insertEventType(eventType1);
        insertEventType(eventType2);
    }

    @Test(expected = NoSuchEventTypeException.class)
    public void whenEventDoesntExistsFindByNameReturnsNothing() throws NakadiException {
        repository.findByName("inexisting-name");
    }

    @Test
    public void whenUpdateExistingEventTypeItUpdates() throws NakadiException, IOException {
        final EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);

        eventType.setCategory(EventCategory.BUSINESS);
        eventType.setOwningApplication("new-application");

        repository.update(eventType);

        final SqlRowSet rs = template.queryForRowSet("SELECT et_name, et_event_type_object FROM zn_data.event_type");
        rs.next();

        assertThat("Name is persisted", rs.getString(1), equalTo(eventType.getName()));

        final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();
        final EventType persisted = mapper.readValue(rs.getString(2), EventType.class);

        assertThat(persisted.getCategory(), equalTo(eventType.getCategory()));
        assertThat(persisted.getOwningApplication(), equalTo(eventType.getOwningApplication()));
        assertThat(persisted.getPartitionKeyFields(), equalTo(eventType.getPartitionKeyFields()));
        assertThat(persisted.getName(), equalTo(eventType.getName()));
        assertThat(persisted.getSchema().getType(), equalTo(eventType.getSchema().getType()));
        assertThat(persisted.getSchema().getSchema(), equalTo(eventType.getSchema().getSchema()));
    }

    @Test
    public void whenListExistingEventTypesAreListed() throws NakadiException {
        final EventType eventType1 = buildDefaultEventType();
        final EventType eventType2 = buildDefaultEventType();

        repository.saveEventType(eventType1);
        repository.saveEventType(eventType2);

        final List<EventType> eventTypes = repository.list();

        assertThat(eventTypes, hasSize(2));
    }

    @Test
    public void whenSetDeletedThenSetDeletedInDatabase() throws Exception {
        final EventType eventType = buildDefaultEventType();
        insertEventType(eventType);

        repository.setEventTypeDeleted(eventType.getName());

        final EventType eventTypeInDb = repository.findByName(eventType.getName());
        Assert.assertEquals(true, eventTypeInDb.isDeleted());
    }

    private void insertEventType(final EventType eventType) throws Exception {
        final String insertSQL = "INSERT INTO zn_data.event_type (et_name, et_topic, et_event_type_object) " +
                "VALUES (?, ?, to_json(?::json))";
        template.update(insertSQL,
                eventType.getName(),
                eventType.getTopic(),
                mapper.writer().writeValueAsString(eventType));
    }
}
