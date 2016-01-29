package de.zalando.aruha.nakadi.problem;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Optional;

public class InvalidJsonProblem implements Problem {

    static final String TYPE_VALUE = "https://httpstatuses.com/422";
    static final URI TYPE = URI.create(TYPE_VALUE);

    private final String title;
    private final String detail;

    public InvalidJsonProblem(String title, String detail) {
        this.title = title;
        this.detail = detail;
    }

    @Override
    public URI getType() {
        return TYPE;
    }

    @Override
    public String getTitle() {
        return this.title;
    }

    @Override
    @JsonIgnore // Hack required since Jdk8Module serializers seems not to work as expected
    public Optional<String> getDetail() {
        return Optional.of(this.detail);
    }

    // Hack required since Jdk8Module serializers seems not to work as expected
    @JsonProperty("detail")
    public String getFieldName() {
        return detail;
    }

    @Override
    @JsonIgnore // Hack required since Jdk8Module serializers seems not to work as expected
    public Optional<URI> getInstance() {
        return Optional.empty();
    }

    @Override
    @JsonIgnore // Hack required since Jdk8Module serializers seems not to work as expected
    public ImmutableMap<String, Object> getParameters() {
        return ImmutableMap.of();
    }

    @Override
    public Response.StatusType getStatus() {
        return MoreStatus.UNPROCESSABLE_ENTITY;
    }
}
