package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;

@RestController
@RequestMapping(value = "/version", produces = APPLICATION_JSON_VALUE)
@Profile("!test")
public class VersionController {

    private static final String SCM_SOURCE_FILE = "/scm-source.json";

    private static class ScmSource {
        private String author;
        private String revision;
        private String status;
        private String url;

        public String getAuthor() {
            return author;
        }

        public void setAuthor(final String author) {
            this.author = author;
        }

        public String getRevision() {
            return revision;
        }

        public void setRevision(final String revision) {
            this.revision = revision;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(final String status) {
            this.status = status;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(final String url) {
            this.url = url;
        }
    }

    public static class VersionInfo {
        private final ScmSource scmSource;

        public VersionInfo(final ScmSource scmSource) {
            this.scmSource = scmSource;
        }

        public ScmSource getScmSource() {
            return scmSource;
        }

    }

    private final VersionInfo versionInfo;
    private static final Logger LOG = LoggerFactory.getLogger(EventStreamController.class);

    @Autowired
    public VersionController(final ObjectMapper objectMapper) {
        this.versionInfo = new VersionInfo(loadScmSource(objectMapper));
    }

    @RequestMapping(method = GET)
    public VersionInfo getVersion() {
        return versionInfo;
    }

    private static ScmSource loadScmSource(final ObjectMapper objectMapper) {
        try (InputStream in = new FileInputStream(SCM_SOURCE_FILE)) {
            return objectMapper.readValue(in, ScmSource.class);
        } catch (IOException ex) {
            LOG.warn("Failed to read scm-source.json file from " + SCM_SOURCE_FILE, ex);
        }
        return null;
    }

}
