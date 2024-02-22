package org.zalando.nakadi.config;

import com.lightstep.tracer.jre.JRETracer;
import com.lightstep.tracer.shared.Options;
import io.opentracing.Tracer;
import io.opentracing.noop.NoopTracerFactory;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

@Configuration
public class LightstepConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(LightstepConfiguration.class);

    @Value("${opentracing.lightstep.access_token}")
    String accessToken;

    @Value("${opentracing.lightstep.component_name}")
    String componentName;

    @Value("${opentracing.lightstep.collector_host}")
    String collectorHost;

    @Value("${opentracing.lightstep.collector_port}")
    int collectorPort;

    @Value("${opentracing.lightstep.max_buffered_spans:1000}")
    int maxBufferedSpans;

    @Value("${opentracing.lightstep.max_reporting_interval_ms:2500}")
    int maxReportingIntervalMs;

    @Value("${opentracing.lightstep.verbosity:1}")
    int verbosity;

    @Value("${application.version:none}")
    String applicationVersion;

    @Value("${application.id:none}")
    String applicationId;

    @Value("${hostname:none}")
    String hostname;

    @Value("${.platform.opentracing.tag.deployment_id:none}")
    String deploymentId;

    @Bean
    @Primary
    public Tracer tracer() {
        try {
            final Options options = new Options.OptionsBuilder()
                    .withAccessToken(accessToken)
                    .withCollectorHost(collectorHost)
                    .withCollectorPort(collectorPort)
                    .withComponentName(componentName)
                    .withMaxBufferedSpans(maxBufferedSpans)
                    .withMaxReportingIntervalMillis(maxReportingIntervalMs)
                    .withVerbosity(verbosity)
                    .withTag("artifact_version", applicationVersion)
                    .withTag("peer.hostname", hostname)
                    .withTag("peer.service", applicationId)
                    .withTag("deployment_id", deploymentId)
                    .build();

            final Tracer tracer = new JRETracer(options);
            GlobalTracer.register(tracer);

            LOG.info("Initialized Lightstep Tracer: {}", this);
            return tracer;
        } catch (Exception ex) {
            LOG.error("Invalid Lightstep configuration.  Returning a NoopTracer: {}", ex);
            return NoopTracerFactory.create();
        }
    }

    @Override
    public String toString() {
        return "LightstepConfiguration{" +
                "componentName='" + componentName + '\'' +
                ", collectorHost='" + collectorHost + '\'' +
                ", collectorPort=" + collectorPort +
                ", maxBufferedSpans=" + maxBufferedSpans +
                ", maxReportingIntervalMs=" + maxReportingIntervalMs +
                ", verbosity=" + verbosity +
                ", applicationVersion='" + applicationVersion + '\'' +
                ", applicationId='" + applicationId + '\'' +
                ", hostname='" + hostname + '\'' +
                ", deploymentId='" + deploymentId + '\'' +
                '}';
    }
}
