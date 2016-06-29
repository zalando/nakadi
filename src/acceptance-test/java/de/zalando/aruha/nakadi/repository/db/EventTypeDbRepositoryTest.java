package de.zalando.aruha.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.EventCategory;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import de.zalando.aruha.nakadi.exceptions.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import java.io.IOException;
import java.util.List;

import static de.zalando.aruha.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

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
        EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.event_type", Integer.class);
        assertThat("Number of rows should increase", rows, equalTo(1));

        SqlRowSet rs = template.queryForRowSet("SELECT et_name, et_topic, et_event_type_object FROM zn_data.event_type");
        rs.next();

        assertThat("Name is persisted", rs.getString(1), equalTo(eventType.getName()));
        assertThat("Topic is persisted", rs.getString(2), equalTo(eventType.getTopic()));

        ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();
        EventType persisted = mapper.readValue(rs.getString(3), EventType.class);

        assertThat(persisted.getCategory(), equalTo(eventType.getCategory()));
        assertThat(persisted.getName(), equalTo(eventType.getName()));
        assertThat(persisted.getSchema().getType(), equalTo(eventType.getSchema().getType()));
        assertThat(persisted.getSchema().getSchema(), equalTo(eventType.getSchema().getSchema()));
    }

    @Test(expected = DuplicatedEventTypeNameException.class)
    public void whenCreateDuplicatedNamesThrowAnError() throws Exception {
        EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);
        repository.saveEventType(eventType);
    }

    @Test
    public void whenEventExistsFindByNameReturnsSomething() throws Exception {
        EventType eventType1 = buildDefaultEventType();
        EventType eventType2 = buildDefaultEventType();

        insertEventType(eventType1);
        insertEventType(eventType2);

        EventType persistedEventType = repository.findByName(eventType2.getName());

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
    public void whenEventDoesntExistsFindByNameReturnsNothing() throws NakadiException, NoSuchEventTypeException {
        repository.findByName("inexisting-name");
    }

    @Test
    public void whenUpdateExistingEventTypeItUpdates() throws NakadiException, DuplicatedEventTypeNameException, IOException, NoSuchEventTypeException {
        EventType eventType = buildDefaultEventType();

        repository.saveEventType(eventType);

        eventType.setCategory(EventCategory.BUSINESS);
        eventType.setOwningApplication("new-application");

        repository.update(eventType);

        SqlRowSet rs = template.queryForRowSet("SELECT et_name, et_event_type_object FROM zn_data.event_type");
        rs.next();

        assertThat("Name is persisted", rs.getString(1), equalTo(eventType.getName()));

        ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();
        EventType persisted = mapper.readValue(rs.getString(2), EventType.class);

        assertThat(persisted.getCategory(), equalTo(eventType.getCategory()));
        assertThat(persisted.getOwningApplication(), equalTo(eventType.getOwningApplication()));
        assertThat(persisted.getPartitionKeyFields(), equalTo(eventType.getPartitionKeyFields()));
        assertThat(persisted.getName(), equalTo(eventType.getName()));
        assertThat(persisted.getSchema().getType(), equalTo(eventType.getSchema().getType()));
        assertThat(persisted.getSchema().getSchema(), equalTo(eventType.getSchema().getSchema()));
    }

    @Test
    public void whenListExistingEventTypesAreListed() throws NakadiException, DuplicatedEventTypeNameException {
        EventType eventType1 = buildDefaultEventType();
        EventType eventType2 = buildDefaultEventType();

        repository.saveEventType(eventType1);
        repository.saveEventType(eventType2);

        List<EventType> eventTypes = repository.list();

        assertThat(eventTypes, hasSize(2));
    }

    @Test
    public void whenRemoveThenDeleteFromDatabase() throws Exception {
        EventType eventType = buildDefaultEventType();
        insertEventType(eventType);

        repository.removeEventType(eventType.getName());

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.event_type", Integer.class);
        assertThat("Number of rows should encrease", rows, equalTo(0));
    }

    private void insertEventType(EventType eventType) throws Exception {
        String insertSQL = "INSERT INTO zn_data.event_type (et_name, et_topic, et_event_type_object) VALUES (?, ?, to_json(?::json))";
        template.update(insertSQL,
                eventType.getName(),
                eventType.getTopic(),
                mapper.writer().writeValueAsString(eventType));
    }
}
