package de.zalando.aruha.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.config.NakadiConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.NoSuchEventTypeException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;

public class EventTypeDbRepositoryTest {

    private JdbcTemplate template;
    private EventTypeRepository repository;
    private Connection connection;
    private ObjectMapper mapper;

    private static final String postgresqlUrl = "jdbc:postgresql://localhost:5432/local_nakadi_db";
    private static final String username = "nakadi_app";
    private static final String password = "nakadi";

    @Before
    public void setUp() {
        try {
            mapper = (new NakadiConfig()).jacksonObjectMapper();

            DataSource datasource = new DriverManagerDataSource(postgresqlUrl, username, password);
            template = new JdbcTemplate(datasource);
            repository = new EventTypeDbRepository(template, mapper);
            connection = datasource.getConnection();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void whenCreateNewEventTypePersistItInTheDatabase() throws Exception {
        EventType eventType = buildEventType();

        repository.saveEventType(eventType);

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.event_type", Integer.class);
        assertThat("Number of rows should encrease", rows, equalTo(1));

        SqlRowSet rs = template.queryForRowSet("SELECT et_name, et_event_type_object FROM zn_data.event_type");
        rs.next();

        assertThat("Name is persisted", rs.getString(1), equalTo("event-name"));

        ObjectMapper mapper = (new NakadiConfig()).jacksonObjectMapper();
        EventType persisted = mapper.readValue(rs.getString(2), EventType.class);

        assertThat(persisted.getCategory(), equalTo(eventType.getCategory()));
        assertThat(persisted.getName(), equalTo(eventType.getName()));
        assertThat(persisted.getEventTypeSchema().getType(), equalTo(eventType.getEventTypeSchema().getType()));
        assertThat(persisted.getEventTypeSchema().getSchema(), equalTo(eventType.getEventTypeSchema().getSchema()));
    }

    @Test(expected = DuplicatedEventTypeNameException.class)
    public void whenCreateDuplicatedNamesThrowAnError() throws Exception {
        EventType eventType = buildEventType();

        repository.saveEventType(eventType);
        repository.saveEventType(eventType);
    }

    @Test
    public void whenEventExistsFindByNameReturnsSomething() throws Exception {
        EventType eventType1 = buildEventType();
        EventType eventType2 = buildEventType();
        eventType2.setName("event-name-2");

        insertEventType(eventType1);
        insertEventType(eventType2);

        EventType persistedEventType = repository.findByName(eventType2.getName());

        assertThat(persistedEventType, notNullValue());
    }

    @Test(expected = NoSuchEventTypeException.class)
    public void whenEventDoesntExistsFindByNameReturnsNothing() throws NakadiException, NoSuchEventTypeException {
        repository.findByName("inexisting-name");
    }

    @Test
    public void whenUpdateExistingEventTypeItUpdates() throws NakadiException, DuplicatedEventTypeNameException, IOException, NoSuchEventTypeException {
        EventType eventType = buildEventType();

        repository.saveEventType(eventType);

        eventType.setCategory("new-category");

        repository.update(eventType);

        SqlRowSet rs = template.queryForRowSet("SELECT et_name, et_event_type_object FROM zn_data.event_type");
        rs.next();

        assertThat("Name is persisted", rs.getString(1), equalTo("event-name"));

        ObjectMapper mapper = (new NakadiConfig()).jacksonObjectMapper();
        EventType persisted = mapper.readValue(rs.getString(2), EventType.class);

        assertThat(persisted.getCategory(), equalTo(eventType.getCategory()));
        assertThat(persisted.getName(), equalTo(eventType.getName()));
        assertThat(persisted.getEventTypeSchema().getType(), equalTo(eventType.getEventTypeSchema().getType()));
        assertThat(persisted.getEventTypeSchema().getSchema(), equalTo(eventType.getEventTypeSchema().getSchema()));
    }

    @Test
    public void whenListExistingEventTypesAreListed() throws NakadiException, DuplicatedEventTypeNameException {
        EventType eventType1 = buildEventType();
        EventType eventType2 = buildEventType();
        eventType2.setName("event-name-2");

        repository.saveEventType(eventType1);
        repository.saveEventType(eventType2);

        List<EventType> eventTypes = repository.list();

        assertThat(eventTypes, hasSize(2));
    }

    @Test
    public void whenRemoveThenDeleteFromDatabase() throws Exception {
        EventType eventType = buildEventType();
        insertEventType(eventType);

        repository.removeEventType(eventType.getName());

        final int rows = template.queryForObject("SELECT count(*) FROM zn_data.event_type", Integer.class);
        assertThat("Number of rows should encrease", rows, equalTo(0));
    }

    private void insertEventType(EventType eventType) throws Exception {
        String insertSQL = "INSERT INTO zn_data.event_type (et_name, et_event_type_object) VALUES (?, to_json(?::json))";
        template.update(insertSQL,
                eventType.getName(),
                mapper.writer().writeValueAsString(eventType));
    }

    private EventType buildEventType() {
        final EventTypeSchema schema = new EventTypeSchema();
        final EventType eventType = new EventType();

        schema.setSchema("{ \"price\": 1000 }");
        schema.setType(EventTypeSchema.Type.JSON_SCHEMA);

        eventType.setName("event-name");
        eventType.setCategory("event-category");
        eventType.setEventTypeSchema(schema);

        return eventType;
    }

    @After
    public void tearDown() throws SQLException {
        template.execute("DELETE FROM zn_data.event_type");

        connection.close();
    }

}
