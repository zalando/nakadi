package org.zalando.nakadi.service;

import org.apache.log4j.PropertyConfigurator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.FileCopyUtils;

import javax.annotation.PostConstruct;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Service
public class LoggingReconfigurator {
    private final Resource resource;
    private volatile String lastFile;

    private static final Logger LOG = LoggerFactory.getLogger(LoggingReconfigurator.class);

    @Autowired
    public LoggingReconfigurator(
            @Value("${nakadi.logging.configuration:classpath:log4j.properties}") final Resource configuration) {
        this.resource = configuration;
    }

    @PostConstruct
    public void postConstruct() {
        reloadConfigurationIfChanged();
    }

    @Scheduled(fixedDelay = 10_000)
    public void checkConfigurationChange() {
        LOG.trace("Checking if configuration is changed");
        reloadConfigurationIfChanged();
    }

    private synchronized void reloadConfigurationIfChanged() {
        try (InputStream in = resource.getInputStream()) {
            final byte[] binaryData = FileCopyUtils.copyToByteArray(in);
            final String fileData = new String(binaryData, StandardCharsets.UTF_8);
            if (!Objects.equals(fileData, lastFile)) {
                LOG.info("Logging configuration changed, reloading ({})", resource);
                PropertyConfigurator.configure(new ByteArrayInputStream(binaryData));
                lastFile = fileData;
            }
        } catch (IOException ex) {
            LOG.warn("Failed to read logging configuration from resource {}", resource, ex);
        }
    }
}
