package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;

import javax.annotation.concurrent.Immutable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.List;

@Immutable
public class EventTypeAuthorization {

    @NotNull
    @Valid
    @Size(min = 1, message = "must contain at least one attribute")
    private final List<AuthorizationAttribute> admins;

    @NotNull
    @Valid
    @Size(min = 1, message = "must contain at least one attribute")
    private final List<AuthorizationAttribute> readers;

    @NotNull
    @Valid
    @Size(min = 1, message = "must contain at least one attribute")
    private final List<AuthorizationAttribute> writers;

    public EventTypeAuthorization(@JsonProperty("admins") final List<AuthorizationAttribute> admins,
                                  @JsonProperty("readers") final List<AuthorizationAttribute> readers,
                                  @JsonProperty("writers") final List<AuthorizationAttribute> writers) {
        this.admins = admins;
        this.readers = readers;
        this.writers = writers;
    }

    public List<AuthorizationAttribute> getAdmins() {
        return Collections.unmodifiableList(admins);
    }

    public List<AuthorizationAttribute> getReaders() {
        return Collections.unmodifiableList(readers);
    }

    public List<AuthorizationAttribute> getWriters() {
        return Collections.unmodifiableList(writers);
    }
}
