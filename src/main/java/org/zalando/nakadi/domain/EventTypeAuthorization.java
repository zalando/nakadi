package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

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
    private final List<EventTypeAuthorizationAttribute> admins;

    @NotNull
    @Valid
    @Size(min = 1, message = "must contain at least one attribute")
    private final List<EventTypeAuthorizationAttribute> readers;

    @NotNull
    @Valid
    @Size(min = 1, message = "must contain at least one attribute")
    private final List<EventTypeAuthorizationAttribute> writers;

    public EventTypeAuthorization(@JsonProperty("admins") final List<EventTypeAuthorizationAttribute> admins,
                                  @JsonProperty("readers") final List<EventTypeAuthorizationAttribute> readers,
                                  @JsonProperty("writers") final List<EventTypeAuthorizationAttribute> writers) {
        // actually these three properties should never be null but the validation framework first creates an object
        // and then uses getters to check if values are null or not so we need to do this check to avoid exception
        this.admins = admins == null ? null : Collections.unmodifiableList(admins);
        this.readers = readers == null ? null : Collections.unmodifiableList(readers);
        this.writers = writers == null ? null : Collections.unmodifiableList(writers);
    }

    public List<EventTypeAuthorizationAttribute> getAdmins() {
        return admins;
    }

    public List<EventTypeAuthorizationAttribute> getReaders() {
        return readers;
    }

    public List<EventTypeAuthorizationAttribute> getWriters() {
        return writers;
    }
}
