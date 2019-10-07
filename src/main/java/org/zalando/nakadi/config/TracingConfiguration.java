package org.zalando.nakadi.config;

import com.lightstep.tracer.jre.JRETracer;
import com.lightstep.tracer.shared.Options;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class TracingConfiguration {
    private static final Logger LOG = LoggerFactory.getLogger(TracingConfiguration.class);
    @Value("${nakadi.tracing.componentName}")
    private String componentName;
    @Value("${nakadi.tracing.accessToken}")
    private String accessToken;
    @Value("${nakadi.tracing.collectorHost}")
    private String collectorHost;
    @Value("${nakadi.tracing.collectorPort}")
    private int collectorPort;

    @Bean
    public Tracer tracer() {
        try {
            final Options options = new Options.OptionsBuilder()
                    .withAccessToken(accessToken)
                    .withCollectorHost(collectorHost)
                    .withCollectorPort(collectorPort)
                    .withComponentName(componentName)
                    .build();

            final Tracer tracer = new JRETracer(options);
            LOG.info("Initialized Lightstep Tracer");
            return tracer;
        } catch (Exception ex) {
            LOG.error("Invalid Lightstep configuration. Returning a NoopTracer. {}", ex.getMessage());
            return NoopTracerFactory.create();
        }
    }
}
