package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.parsing.Parser;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.repository.db.EventTypeDbRepository;
import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import static de.zalando.aruha.nakadi.utils.TestUtils.buildDefaultEventType;

public abstract class BaseAT {

    protected static final int PORT = 8080;
    protected static final String URL = "http://localhost:" + PORT;

    protected static final String ZOOKEEPER_URL = "localhost:2181";
    protected static final String KAFKA_URL = "localhost:9092";

    private static final String postgresqlUrl = "jdbc:postgresql://localhost:5432/local_nakadi_db";
    private static final String username = "nakadi_app";
    private static final String password = "nakadi";

    protected static final String EVENT_TYPE_NAME = "test-event-type-name";
    protected static final String TEST_TOPIC = "test-topic";
    protected static final int PARTITIONS_NUM = 8;

    private static final JdbcTemplate template = new JdbcTemplate(
            new DriverManagerDataSource(postgresqlUrl, username, password));
    private static final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();
    private static final EventTypeDbRepository eventTypeRepo = new EventTypeDbRepository(template, mapper);

    static {
        RestAssured.port = PORT;
        RestAssured.defaultParser = Parser.JSON;
    }

    @BeforeClass
    public static void createTestEventType() throws Exception {
        try {
            eventTypeRepo.findByName(EVENT_TYPE_NAME);
        } catch (final NoSuchEventTypeException e) {
            EventType eventType = buildDefaultEventType();
            eventType.setName(EVENT_TYPE_NAME);
            eventType.setTopic(TEST_TOPIC);
            eventTypeRepo.saveEventType(eventType);
        }
    }
}
