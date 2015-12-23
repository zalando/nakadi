package de.zalando.aruha.nakadi.config;

import javax.sql.DataSource;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;

@Configuration
@Profile("development")
@PropertySource("${nakadi.config}")
public class DevelopmentConfig implements Config {
    @Override
    public DataSource dataSource() {
        return null;
    }
}
