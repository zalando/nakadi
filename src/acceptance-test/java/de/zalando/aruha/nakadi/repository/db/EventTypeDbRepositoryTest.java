package de.zalando.aruha.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.config.NakadiConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import javax.sql.DataSource;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Optional;

import static de.zalando.aruha.nakadi.utils.IsOptional.isAbsent;
import static de.zalando.aruha.nakadi.utils.IsOptional.isPresent;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class EventTypeDbRepositoryTest {

    private DataSource datasource;
    private JdbcTemplate template;
    private EventTypeRepository repository;
    private Connection connection;

    private static final String postgresqlUrl = "jdbc:postgresql://localhost:5432/local_nakadi_db";
    private static final String username = "nakadi_app";
    private static final String password = "nakadi";

    @Before
    public void setUp() {
        try {
            ObjectMapper mapper = (new NakadiConfig()).jacksonObjectMapper();

            datasource = new DriverManagerDataSource(postgresqlUrl, username, password);
            template = new JdbcTemplate(datasource);
            repository = new EventTypeDbRepository(template, mapper);
            connection = datasource.getConnection();

            template.execute("DELETE FROM zn_data.event_type");
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
    public void whenEventExistsFindByNameReturnsSomething() throws NakadiException, DuplicatedEventTypeNameException {
        EventType eventType = buildEventType();

        repository.saveEventType(eventType);

        Optional<EventType> eventTypeOptional = repository.findByName(eventType.getName());

        assertThat(eventTypeOptional, isPresent());
    }

    @Test
    public void whenEventDoesntExistsFindByNameReturnsNothing() throws NakadiException {
        Optional<EventType> eventTypeOptional = repository.findByName("inexisting-name");

        assertThat(eventTypeOptional, isAbsent());
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
