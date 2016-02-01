package de.zalando.aruha.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.config.NakadiConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
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
import java.sql.Connection;
import java.sql.SQLException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

public class EventTypeDbRepositoryTest {

    private DataSource datasource;
    private JdbcTemplate template;
    private EventTypeRepository repository;
    private Connection connection;

    private static final String postgresqlUrl = "jdbc:postgresql://localhost:5432/local_schemaregistry_db";
    private static final String username = "schemaregistry";
    private static final String password = "schemaregistry";

    @Before
    public void setUp() {
        try {
            ObjectMapper mapper = (new NakadiConfig()).jacksonObjectMapper();

            datasource = new DriverManagerDataSource(postgresqlUrl, username, password);
            template = new JdbcTemplate(datasource);
            repository = new EventTypeDbRepository(template, mapper);
            connection = datasource.getConnection();

            ResourceDatabasePopulator rdp = new ResourceDatabasePopulator();
            rdp.addScript(new ClassPathResource("schema.sql"));
            rdp.populate(connection);

            template.execute("TRUNCATE TABLE zsr_data.event_type");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void whenCreateNewEventTypePersistItInTheDatabase() throws Exception {
        EventType eventType = buildEventType();

        repository.saveEventType(eventType);

        final int rows = template.queryForObject("SELECT count(*) FROM zsr_data.event_type", Integer.class);
        assertThat("Number of rows should encrease", rows, equalTo(1));

        SqlRowSet rs = template.queryForRowSet("SELECT et_name, et_event_type_object FROM zsr_data.event_type");
        rs.next();

        assertThat("Name is persisted", rs.getString(1), equalTo("event-name"));
        assertThat("Schema is persisted", rs.getString(2), equalTo("{\"name\": \"event-name\", \"type\": null, \"topic\": null, \"schema\": {\"type\": null, \"schema\": {\"price\": 1000}}, \"owning_application\": null}"));
    }

    @Test(expected = DuplicateKeyException.class)
    public void whenCreateDuplicatedNamesThrowAnError() throws Exception {
        EventType eventType = buildEventType();

        repository.saveEventType(eventType);
        repository.saveEventType(eventType);
    }

    private EventType buildEventType() {
        final EventTypeSchema schema = new EventTypeSchema();
        final EventType eventType = new EventType();
        schema.setSchema(new JSONObject("{ \"price\": 1000 }"));
        eventType.setName("event-name");
        eventType.setEventTypeSchema(schema);

        return eventType;
    }

    @After
    public void tearDown() throws SQLException {
        template.execute("TRUNCATE TABLE zsr_data.event_type");

        connection.close();
    }

}
