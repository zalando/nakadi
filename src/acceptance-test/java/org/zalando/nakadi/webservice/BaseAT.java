package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.parsing.Parser;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.http.params.CoreConnectionPNames;
import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.db.EventTypeDbRepository;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public abstract class BaseAT {

    public static final String POSTGRES_URL = "jdbc:postgresql://localhost:5432/local_nakadi_db";
    public static final String POSTGRES_USER = "nakadi";
    public static final String POSTGRES_PWD = "nakadi";

    protected static final int PORT = 8080;
    public static final String URL = "http://localhost:" + PORT;

    protected static final String ZOOKEEPER_URL = "localhost:2181";
    protected static final String KAFKA_URL = "localhost:9092";

    protected static final String EVENT_TYPE_NAME = "test-event-type-name";
    protected static final String TEST_TOPIC = "test-topic";
    protected static final EventType EVENT_TYPE = EventTypeTestBuilder.builder()
            .name(EVENT_TYPE_NAME)
            .topic(TEST_TOPIC).build();
    protected static final int PARTITIONS_NUM = 8;

    private static final JdbcTemplate JDBC_TEMPLATE = new JdbcTemplate(
            new DriverManagerDataSource(POSTGRES_URL, POSTGRES_USER, POSTGRES_PWD));
    protected static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();
    protected static final EventTypeDbRepository EVENT_TYPE_REPO = new EventTypeDbRepository(JDBC_TEMPLATE, MAPPER);
    protected static final StorageDbRepository STORAGE_DB_REPOSITORY = new StorageDbRepository(JDBC_TEMPLATE, MAPPER);
    protected static final TimelineDbRepository TIMELINE_REPOSITORY = new TimelineDbRepository(JDBC_TEMPLATE, MAPPER);

    static {
        RestAssured.port = PORT;
        RestAssured.defaultParser = Parser.JSON;
        RestAssured.config().getHttpClientConfig().setParam(CoreConnectionPNames.SO_TIMEOUT, TimeUnit.SECONDS.toMillis(3));
        RestAssured.config().getHttpClientConfig().setParam(CoreConnectionPNames.CONNECTION_TIMEOUT, 100);
    }

    @BeforeClass
    public static void initDB() throws Exception {
        try {
            EVENT_TYPE_REPO.findByName(EVENT_TYPE_NAME);
        } catch (final NoSuchEventTypeException e) {
            final EventType eventType = buildDefaultEventType();
            eventType.setName(EVENT_TYPE_NAME);
            eventType.setTopic(TEST_TOPIC);
            EVENT_TYPE_REPO.saveEventType(eventType);
        }

        final Optional<Storage> defaultStorage = STORAGE_DB_REPOSITORY.getStorage("default");
        if (!defaultStorage.isPresent()) {
            final Storage storage = new Storage();
            storage.setId("default");
            storage.setType(Storage.Type.KAFKA);
            storage.setConfiguration(new Storage.KafkaConfiguration(null, null, ZOOKEEPER_URL, ""));
            STORAGE_DB_REPOSITORY.createStorage(storage);
        }
    }
}
