package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.List;

@Immutable
public class EventTypeAuthorization {

    @NotNull
    @Size(min = 1, message = "must contain at least one attribute")
    private final List<EventTypeAuthorizationAttribute> admins;

    @NotNull
    @Size(min = 1, message = "must contain at least one attribute")
    private final List<EventTypeAuthorizationAttribute> readers;

    @NotNull
    @Size(min = 1, message = "must contain at least one attribute")
    private final List<EventTypeAuthorizationAttribute> writers;

    public EventTypeAuthorization(@JsonProperty("admins") final List<EventTypeAuthorizationAttribute> admins,
                                  @JsonProperty("readers") final List<EventTypeAuthorizationAttribute> readers,
                                  @JsonProperty("writers") final List<EventTypeAuthorizationAttribute> writers) {
        this.admins = admins;
        this.readers = readers;
        this.writers = writers;
    }

    public List<EventTypeAuthorizationAttribute> getAdmins() {
        return Collections.unmodifiableList(admins);
    }

    public List<EventTypeAuthorizationAttribute> getReaders() {
        return Collections.unmodifiableList(readers);
    }

    public List<EventTypeAuthorizationAttribute> getWriters() {
        return Collections.unmodifiableList(writers);
    }
}
