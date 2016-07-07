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

    private static final String POSTGRESQL_URL = "jdbc:postgresql://localhost:5432/local_nakadi_db";
    private static final String USERNAME = "nakadi_app";
    private static final String PASSWORD = "nakadi";

    protected static final String EVENT_TYPE_NAME = "test-event-type-name";
    protected static final String TEST_TOPIC = "test-topic";
    protected static final int PARTITIONS_NUM = 8;

    private static final JdbcTemplate JDBC_TEMPLATE = new JdbcTemplate(
            new DriverManagerDataSource(POSTGRESQL_URL, USERNAME, PASSWORD));
    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    private static final EventTypeDbRepository EVENT_TYPE_REPO = new EventTypeDbRepository(JDBC_TEMPLATE, MAPPER);

    static {
        RestAssured.port = PORT;
        RestAssured.defaultParser = Parser.JSON;
    }

    @BeforeClass
    public static void createTestEventType() throws Exception {
        try {
            EVENT_TYPE_REPO.findByName(EVENT_TYPE_NAME);
        } catch (final NoSuchEventTypeException e) {
            EventType eventType = buildDefaultEventType();
            eventType.setName(EVENT_TYPE_NAME);
            eventType.setTopic(TEST_TOPIC);
            EVENT_TYPE_REPO.saveEventType(eventType);
        }
    }
}
