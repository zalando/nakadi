package de.zalando.aruha.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import javax.ws.rs.core.MediaType;
import org.codehaus.jackson.annotate.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.RequestMapping;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/version", produces = MediaType.APPLICATION_JSON)
public class VersionController {

    private static final String SCM_SOURCE_FILE = "/scm-source.json";

    private static class ScmSource {
        @JsonProperty("author")
        private String author;
        @JsonProperty("revision")
        private String revision;
        @JsonProperty("status")
        private String status;
        @JsonProperty("url")
        private String url;

        public String getAuthor() {
            return author;
        }

        public void setAuthor(String author) {
            this.author = author;
        }

        public String getRevision() {
            return revision;
        }

        public void setRevision(String revision) {
            this.revision = revision;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public String getUrl() {
            return url;
        }

        public void setUrl(String url) {
            this.url = url;
        }
    }

    private static class VersionInfo {
        private final ScmSource scmSource;

        public VersionInfo(ScmSource scmSource) {
            this.scmSource = scmSource;
        }

        public ScmSource getScmSource() {
            return scmSource;
        }

    }

    private final VersionInfo versionInfo;
    private static final Logger LOG = LoggerFactory.getLogger(EventStreamController.class);

    public VersionController(final ObjectMapper objectMapper) {
        this.versionInfo = new VersionInfo(loadScmSource(objectMapper));
    }

    @RequestMapping(method = GET)
    public VersionInfo getVersion() {
        return versionInfo;
    }

    private static ScmSource loadScmSource(ObjectMapper objectMapper) {
        try (InputStream in = new FileInputStream(SCM_SOURCE_FILE)) {
            return objectMapper.readValue(in, ScmSource.class);
        } catch (IOException ex) {
            LOG.warn("Failed to read scm-source.json file from " + SCM_SOURCE_FILE, ex);
        }
        return null;
    }

}
