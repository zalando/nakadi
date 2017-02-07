package org.zalando.nakadi.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.zalando.twintip.spring.SchemaResource;

@Configuration
@Import(SchemaResource.class)
public class TwintipConfig {
}
