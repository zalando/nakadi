package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;

import javax.annotation.concurrent.Immutable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Immutable
public class EventTypeAuthorization {

    @NotNull
    @Valid
    @Size(min = 1, message = "must contain at least one attribute")
    @JsonDeserialize(contentAs = EventTypeAuthorizationAttribute.class)
    private final List<AuthorizationAttribute> admins;

    @NotNull
    @Valid
    @Size(min = 1, message = "must contain at least one attribute")
    @JsonDeserialize(contentAs = EventTypeAuthorizationAttribute.class)
    private final List<AuthorizationAttribute> readers;

    @NotNull
    @Valid
    @Size(min = 1, message = "must contain at least one attribute")
    @JsonDeserialize(contentAs = EventTypeAuthorizationAttribute.class)
    private final List<AuthorizationAttribute> writers;

    public EventTypeAuthorization(@JsonProperty("admins") final List<AuthorizationAttribute> admins,
                                  @JsonProperty("readers") final List<AuthorizationAttribute> readers,
                                  @JsonProperty("writers") final List<AuthorizationAttribute> writers) {
        // actually these three properties should never be null but the validation framework first creates an object
        // and then uses getters to check if values are null or not, so we need to do this check to avoid exception
        this.admins = admins == null ? null : Collections.unmodifiableList(admins);
        this.readers = readers == null ? null : Collections.unmodifiableList(readers);
        this.writers = writers == null ? null : Collections.unmodifiableList(writers);
    }

    public List<AuthorizationAttribute> getAdmins() {
        return admins;
    }

    public List<AuthorizationAttribute> getReaders() {
        return readers;
    }

    public List<AuthorizationAttribute> getWriters() {
        return writers;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final EventTypeAuthorization that = (EventTypeAuthorization) o;
        return Objects.equals(admins, that.admins) &&
                Objects.equals(readers, that.readers) &&
                Objects.equals(writers, that.writers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(admins, readers, writers);
    }
}
