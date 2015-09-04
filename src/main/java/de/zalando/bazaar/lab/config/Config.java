package de.zalando.bazaar.lab.config;

import javax.sql.DataSource;

public interface Config {
    DataSource dataSource();
}
