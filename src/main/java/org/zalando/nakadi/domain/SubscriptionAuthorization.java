package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class SubscriptionAuthorization implements ValidatableAuthorization {
    @NotNull
    @Valid
    @Size(min = 1, message = "must contain at least one attribute")
    @JsonDeserialize(contentAs = ResourceAuthorizationAttribute.class)
    private final List<AuthorizationAttribute> admins;
    @NotNull
    @Valid
    @Size(min = 1, message = "must contain at least one attribute")
    @JsonDeserialize(contentAs = ResourceAuthorizationAttribute.class)
    private final List<AuthorizationAttribute> readers;

    public SubscriptionAuthorization(@JsonProperty("admins") final List<AuthorizationAttribute> admins,
                                     @JsonProperty("readers") final List<AuthorizationAttribute> readers) {
        // actually these three properties should never be null but the validation framework first creates an object
        // and then uses getters to check if values are null or not, so we need to do this check to avoid exception
        this.admins = admins == null ? null : Collections.unmodifiableList(admins);
        this.readers = readers == null ? null : Collections.unmodifiableList(readers);
    }

    public List<AuthorizationAttribute> getAdmins() {
        return admins;
    }

    public List<AuthorizationAttribute> getReaders() {
        return readers;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SubscriptionAuthorization that = (SubscriptionAuthorization) o;
        return Objects.equals(admins, that.admins) &&
                Objects.equals(readers, that.readers);
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public Map<String, List<AuthorizationAttribute>> asMapValue() {
        return ImmutableMap.of(
                AuthorizationService.Operation.ADMIN.toString(), getAdmins(),
                AuthorizationService.Operation.READ.toString(), getReaders()
        );
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) throws IllegalArgumentException {
        switch (operation) {
            case READ:
                return Optional.of(getReaders());
            case ADMIN:
                return Optional.of(getAdmins());
            default:
                throw new IllegalArgumentException("Operation " + operation + " is not supported");
        }

    }
}
