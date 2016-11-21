package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.nakadi.config.JsonConfig;
import org.junit.After;
import org.junit.Before;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

import static org.zalando.nakadi.webservice.BaseAT.POSTGRES_PWD;
import static org.zalando.nakadi.webservice.BaseAT.POSTGRES_URL;
import static org.zalando.nakadi.webservice.BaseAT.POSTGRES_USER;

public abstract class AbstractDbRepositoryTest {

    protected JdbcTemplate template;
    protected Connection connection;
    protected ObjectMapper mapper;
    protected String[] dependencyTables;

    public AbstractDbRepositoryTest(final String[] dependencyTables) {
        this.dependencyTables = dependencyTables;
    }

    @Before
    public void setUp() {
        try {
            mapper = (new JsonConfig()).jacksonObjectMapper();
            final DataSource datasource = new DriverManagerDataSource(POSTGRES_URL, POSTGRES_USER, POSTGRES_PWD);
            template = new JdbcTemplate(datasource);
            connection = datasource.getConnection();
            clearRepositoryTable();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws SQLException {
        clearRepositoryTable();
        connection.close();
    }

    private void clearRepositoryTable() {
        for (final String table : dependencyTables) {
            template.execute("DELETE FROM " + table);
        }
    }
}
