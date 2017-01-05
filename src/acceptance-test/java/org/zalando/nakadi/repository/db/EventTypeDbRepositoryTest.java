package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.Version;
import org.zalando.nakadi.exceptions.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.randomUUID;
import static org.zalando.nakadi.utils.TestUtils.randomValidEventTypeName;

public class EventTypeDbRepositoryTest extends AbstractDbRepositoryTest {

    private EventTypeRepository repository;

    public EventTypeDbRepositoryTest() {
        super(new String[]{ "zn_data.event_type_schema", "zn_data.event_type" });
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

    @Test
    public void whenCreateNewEventTypeAlsoInsertIntoSchemaTable() throws Exception {
        final EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.event_type_schema", Integer.class);
        assertThat("Number of rows should increase", rows, equalTo(1));

        final SqlRowSet rs =
                template.queryForRowSet("SELECT ets_event_type_name, ets_schema_object FROM zn_data.event_type_schema");
        rs.next();

        assertThat("Name is persisted", rs.getString(1), equalTo(eventType.getName()));

        final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();
        final EventTypeSchema persisted = mapper.readValue(rs.getString(2), EventTypeSchema.class);

        assertThat(persisted.getVersion(), equalTo(eventType.getSchema().getVersion()));
        assertThat(persisted.getCreatedAt(), notNullValue());
        assertThat(persisted.getSchema(), equalTo(eventType.getSchema().getSchema()));
        assertThat(persisted.getType(), equalTo(eventType.getSchema().getType()));
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

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.event_type_schema", Integer.class);
        assertThat("Number of rows should increase", rows, equalTo(1));

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
    public void whenUpdateDifferentSchemaVersionThenInsertIt() throws NakadiException, IOException {
        final EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);

        eventType.getSchema().setVersion(new Version("1.1.0"));

        repository.update(eventType);

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.event_type_schema", Integer.class);
        assertThat("Number of rows should increase", rows, equalTo(2));
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
    public void whenRemoveThenDeleteFromDatabase() throws Exception {
        final EventType eventType = buildDefaultEventType();
        insertEventType(eventType);

        repository.removeEventType(eventType.getName());

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.event_type", Integer.class);
        assertThat("Number of rows should encrease", rows, equalTo(0));

        final int schemaRows = template.queryForObject("SELECT count(*) FROM zn_data.event_type_schema", Integer.class);
        assertThat("Number of rows should decrease", schemaRows, equalTo(0));
    }

    @Test
    public void unknownAttributesAreIgnoredWhenDesserializing() throws Exception {
        final String eventTypeName = randomValidEventTypeName();
        final String topic = randomUUID();
        final String insertSQL = "INSERT INTO zn_data.event_type (et_name, et_topic, et_event_type_object) " +
                "VALUES (?, ?, to_json(?::json))";
        template.update(insertSQL,
                eventTypeName,
                topic,
                "{\"unknow_attribute\": \"will just be ignored\"}");

        final EventType persistedEventType = repository.findByName(eventTypeName);

        assertThat(persistedEventType, notNullValue());
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
