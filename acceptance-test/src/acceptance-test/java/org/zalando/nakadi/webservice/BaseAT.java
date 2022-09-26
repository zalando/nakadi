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
    public static final String KAFKA_URL;

    protected static final ObjectMapper MAPPER;
    protected static final StorageDbRepository STORAGE_DB_REPOSITORY;
    protected static final TimelineDbRepository TIMELINE_REPOSITORY;

    public static Configuration configs;

    static {

        // Get configurations from automation.yml file
        try {
            configs = new TestConfigurationContext().load();
        } catch (IOException e) {
            e.printStackTrace();
        }

        POSTGRES_URL = configs.getDatabase().getUrl();
        POSTGRES_USER = configs.getDatabase().getUsername();
        POSTGRES_PWD = configs.getDatabase().getPassword();

        JdbcTemplate jdbcTemplate =
            new JdbcTemplate(new DriverManagerDataSource(POSTGRES_URL, POSTGRES_USER, POSTGRES_PWD));
        MAPPER = (new JsonConfig()).jacksonObjectMapper();
        STORAGE_DB_REPOSITORY = new StorageDbRepository(jdbcTemplate, MAPPER);
        TIMELINE_REPOSITORY = new TimelineDbRepository(jdbcTemplate, MAPPER);


        PORT = configs.getApiPort();
        URL = configs.getApiUrl() + ":" + PORT;
        KAFKA_URL = configs.getKafka().getBootstrapServers();
        ZOOKEEPER_URL = configs.getZookeeperUrl();
        ZOOKEEPER_CONNECTION = ZookeeperConnection.valueOf("zookeeper://" + ZOOKEEPER_URL);

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

}
