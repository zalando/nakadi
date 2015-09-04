package de.zalando.bazaar.lab.config;

import javax.sql.DataSource;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

@Configuration
@Profile("development")
public class DevelopmentConfig implements Config {
    @Override
    public DataSource dataSource() {
        return null;
    }
}
