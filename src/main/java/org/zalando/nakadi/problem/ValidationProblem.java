package org.zalando.nakadi.problem;

import com.google.common.base.CaseFormat;
import org.springframework.validation.Errors;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;

import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Optional;

public class ValidationProblem implements Problem {
    private final Errors errors;

    private static final String TYPE_VALUE = "http://httpstatus.es/422";
    private static final URI TYPE = URI.create(TYPE_VALUE);
    private static final String TITLE = "Unprocessable Entity";

    public ValidationProblem(final Errors errors) {
        this.errors = errors;
    }

    @Override
    public URI getType() {
        return TYPE;
    }

    @Override
    public String getTitle() {
        return TITLE;
    }

    @Override
    public Response.StatusType getStatus() {
        return MoreStatus.UNPROCESSABLE_ENTITY;
    }

    @Override
    public Optional<String> getDetail() {
        return Optional.of(buildErrorMessage());
    }

    private String buildErrorMessage() {
        final StringBuilder detailBuilder = new StringBuilder();

        for (final ObjectError error : errors.getAllErrors()) {
            if (error instanceof FieldError) {
                final String fieldName = CaseFormat.UPPER_CAMEL.
                        to(CaseFormat.LOWER_UNDERSCORE, ((FieldError) error).getField());

                detailBuilder.
                        append("Field \"").
                        append(fieldName).
                        append("\" ").
                        append(error.getDefaultMessage()).
                        append("\n");
            } else {
                detailBuilder.append(error.toString());
            }
        }

        return detailBuilder.toString();
    }
}
