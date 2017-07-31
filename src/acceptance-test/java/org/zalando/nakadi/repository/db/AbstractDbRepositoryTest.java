package org.zalando.nakadi.repository.db;

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

    @Before
    public void setUp() throws Exception {
        try {
            final DataSource datasource = new DriverManagerDataSource(POSTGRES_URL, POSTGRES_USER, POSTGRES_PWD);
            template = new JdbcTemplate(datasource);
            connection = datasource.getConnection();
        } catch (final SQLException e) {
            e.printStackTrace();
        }
    }

    @After
    public void tearDown() throws SQLException {
        connection.close();
    }
}
