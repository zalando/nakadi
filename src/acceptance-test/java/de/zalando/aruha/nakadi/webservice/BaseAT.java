package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.parsing.Parser;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import org.junit.Before;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;

import static de.zalando.aruha.nakadi.utils.TestUtils.buildDefaultEventType;

public abstract class BaseAT {

    protected static final int PORT = 8080;
    protected static final String URL = "http://localhost:" + PORT;

    protected static final String zookeeperUrl = "localhost:2181";
    protected static final String kafkaUrl = "localhost:9092";

    private static final String postgresqlUrl = "jdbc:postgresql://localhost:5432/local_nakadi_db";
    private static final String username = "nakadi_app";
    private static final String password = "nakadi";

    protected static final String EVENT_TYPE_NAME = "test-event-type-name";
    protected static final String TEST_TOPIC = "test-topic";
    protected static final int PARTITIONS_NUM = 8;

    private static boolean initialize = true;

    protected JdbcTemplate template;
    protected ObjectMapper mapper;

    static {
        RestAssured.port = PORT;
        RestAssured.defaultParser = Parser.JSON;
    }

    @Before
    public void setUp() throws Exception {
        if(!initialize) return;
        initialize = false;

        final DataSource datasource = new DriverManagerDataSource(postgresqlUrl, username, password);
        template = new JdbcTemplate(datasource);

        EventType eventType = buildDefaultEventType();
        eventType.setName(EVENT_TYPE_NAME);
        eventType.setTopic(TEST_TOPIC);

        mapper = (new JsonConfig()).jacksonObjectMapper();
        String insertSQL = "INSERT INTO zn_data.event_type (et_name, et_topic, et_event_type_object) VALUES (?, ?, to_json(?::json))";
        template.update(insertSQL,
                eventType.getName(),
                eventType.getTopic(),
                mapper.writer().writeValueAsString(eventType));
    }
}
