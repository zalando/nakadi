package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.parsing.Parser;
import java.io.IOException;
import org.apache.http.params.CoreConnectionPNames;
import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.zalando.nakadi.config.Configuration;
import org.zalando.nakadi.config.TestConfigurationContext;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.storage.KafkaConfiguration;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.storage.ZookeeperConnection;
import org.zalando.nakadi.exceptions.runtime.DuplicatedStorageException;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.db.TimelineDbRepository;

import java.util.concurrent.TimeUnit;

public abstract class BaseAT {

    public static final String POSTGRES_URL;
    public static final String POSTGRES_USER;
    public static final String POSTGRES_PWD;

    protected static final int PORT;
    public static final String URL;

    protected static final String ZOOKEEPER_URL;
    protected static final ZookeeperConnection ZOOKEEPER_CONNECTION;
    protected static final String KAFKA_URL;

    protected static final ObjectMapper MAPPER;
    protected static final StorageDbRepository STORAGE_DB_REPOSITORY;
    protected static final TimelineDbRepository TIMELINE_REPOSITORY;

    public static Configuration configs;

    static {

        // Get configs from environment variables or else assign default values
        String dbUrl = System.getenv("POSTGRES_URL");
        POSTGRES_URL = dbUrl != null ? dbUrl : "jdbc:postgresql://localhost:5432/local_nakadi_db";
        String user = System.getenv("POSTGRES_USER");
        POSTGRES_USER = user != null ? user : "nakadi";
        String pwd = System.getenv("POSTGRES_PWD");
        POSTGRES_PWD = pwd != null ? pwd : "nakadi";

        JdbcTemplate jdbcTemplate =
            new JdbcTemplate(new DriverManagerDataSource(POSTGRES_URL, POSTGRES_USER, POSTGRES_PWD));
        MAPPER = (new JsonConfig()).jacksonObjectMapper();
        STORAGE_DB_REPOSITORY = new StorageDbRepository(jdbcTemplate, MAPPER);
        TIMELINE_REPOSITORY = new TimelineDbRepository(jdbcTemplate, MAPPER);

        String port = System.getenv("NAKADI_PORT");
        PORT = (port != null) ? Integer.parseInt(port) : 8081;

        String baseUrl = System.getenv("NAKADI_BASE_URL");
        URL = baseUrl != null ? baseUrl : "http://localhost" + ":" + PORT;

        String zookeeperUrl = System.getenv("ZOOKEEPER_URL");
        ZOOKEEPER_URL = zookeeperUrl != null ? zookeeperUrl : "localhost:2181";
        ZOOKEEPER_CONNECTION = ZookeeperConnection.valueOf("zookeeper://" + ZOOKEEPER_URL);

        String kafkaUrl = System.getenv("KAFKA_URL");
        KAFKA_URL = kafkaUrl != null ? kafkaUrl : "localhost:29092";

        RestAssured.port = PORT;
        RestAssured.defaultParser = Parser.JSON;
        RestAssured.config().getHttpClientConfig()
                .setParam(CoreConnectionPNames.SO_TIMEOUT, TimeUnit.SECONDS.toMillis(30));
        RestAssured.config().getHttpClientConfig().setParam(
                CoreConnectionPNames.CONNECTION_TIMEOUT, TimeUnit.SECONDS.toMillis(5));
    }

    @BeforeClass
    public static void createDefaultStorage() {
        final Storage storage = new Storage();
        storage.setId("default");
        storage.setType(Storage.Type.KAFKA);
        storage.setConfiguration(new KafkaConfiguration(ZOOKEEPER_CONNECTION));
        storage.setDefault(true);
        try {
            STORAGE_DB_REPOSITORY.createStorage(storage);
        } catch (final DuplicatedStorageException ignore) {
        }
    }

    @BeforeClass
    public static void loadExternalConfigs() throws IOException {
        configs = new TestConfigurationContext().load();
    }

}
