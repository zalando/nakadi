package de.zalando.aruha.nakadi.config;

import javax.sql.DataSource;

public interface Config {
    DataSource dataSource();
}
