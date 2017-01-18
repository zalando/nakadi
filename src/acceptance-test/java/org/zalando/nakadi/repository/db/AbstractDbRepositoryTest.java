package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.stream.Stream;
import javax.sql.DataSource;
import org.junit.After;
import org.junit.Before;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.zalando.nakadi.config.JsonConfig;
import static org.zalando.nakadi.webservice.BaseAT.POSTGRES_PWD;
import static org.zalando.nakadi.webservice.BaseAT.POSTGRES_URL;
import static org.zalando.nakadi.webservice.BaseAT.POSTGRES_USER;

public abstract class AbstractDbRepositoryTest {

    protected JdbcTemplate template;
    protected Connection connection;
    protected ObjectMapper mapper;
    protected String[] repositoryTables;

    public AbstractDbRepositoryTest(final String... repositoryTables) {
        this.repositoryTables = repositoryTables;
    }

    @Before
    public void setUp() {
        try {
            mapper = (new JsonConfig()).jacksonObjectMapper();
            final DataSource datasource = new DriverManagerDataSource(POSTGRES_URL, POSTGRES_USER, POSTGRES_PWD);
            template = new JdbcTemplate(datasource);
            connection = datasource.getConnection();
            clearRepositoryTables();
        } catch (final SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws SQLException {
        clearRepositoryTables();
        connection.close();
    }

    private void clearRepositoryTables() {
        Stream.of(repositoryTables)
                .map(table -> "DELETE FROM " + table)
                .forEach(template::execute);
    }
}
