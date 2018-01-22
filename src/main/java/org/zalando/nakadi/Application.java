package org.zalando.nakadi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.zalando.nakadi.config.FeaturesConfig;

@SpringBootApplication
@ComponentScan(basePackages = {"org.zalando.nakadi", "org.zalando.nakadi.config"})
@EnableConfigurationProperties(FeaturesConfig.class)
public class Application {

    public static void main(final String[] args) {
        SpringApplication.run(Application.class, args);
    }
}
