package de.zalando.aruha.nakadi.problem;

import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Optional;

public class DuplicatedEventTypeNameProblem implements Problem {

    static final String TYPE_VALUE = "https://httpstatuses.com/409";
    static final URI TYPE = URI.create(TYPE_VALUE);
    static final String TITLE = "Duplicated event type name";

    private final String name;

    public DuplicatedEventTypeNameProblem(final String name) {
        this.name = name;
    }

    @Override
    public URI getType() { return TYPE; }

    @Override
    public String getTitle() { return TITLE; }

    @Override
    public Optional<String> getDetail() {
        final String message = "The name \"" + name + "\" has already been taken.";
        return Optional.of(message); }

    @Override
    public Response.StatusType getStatus() { return Response.Status.CONFLICT; }
}
