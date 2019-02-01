package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableMap;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import javax.annotation.concurrent.Immutable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Immutable
public class ResourceAuthorization implements ValidatableAuthorization {

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

    @NotNull
    @Valid
    @Size(min = 1, message = "must contain at least one attribute")
    @JsonDeserialize(contentAs = ResourceAuthorizationAttribute.class)
    private final List<AuthorizationAttribute> writers;

    public ResourceAuthorization(@JsonProperty("admins") final List<AuthorizationAttribute> admins,
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

    @JsonIgnore
    public List<AuthorizationAttribute> getAll() {
        return Stream.of(
                Optional.ofNullable(this.readers).orElse(new ArrayList<>()),
                Optional.ofNullable(this.writers).orElse(new ArrayList<>()),
                Optional.ofNullable(this.admins).orElse(new ArrayList<>()))
                .flatMap(Collection::stream).collect(Collectors.toList());
    }

    public List<Permission> toPermissionsList(final String resource) {
        final List<Permission> permissions = admins.stream()
                .map(p -> new Permission(resource, AuthorizationService.Operation.ADMIN, p))
                .collect(Collectors.toList());
        permissions.addAll(readers.stream()
                .map(p -> new Permission(resource, AuthorizationService.Operation.READ, p))
                .collect(Collectors.toList()));
        permissions.addAll(writers.stream()
                .map(p -> new Permission(resource, AuthorizationService.Operation.WRITE, p))
                .collect(Collectors.toList()));
        return permissions;
    }

    public static ResourceAuthorization fromPermissionsList(final List<Permission> permissions) {
        final List<AuthorizationAttribute> admins = permissions.stream()
                .filter(p -> p.getOperation().equals(AuthorizationService.Operation.ADMIN))
                .map(Permission::getAuthorizationAttribute)
                .collect(Collectors.toList());
        final List<AuthorizationAttribute> readers = permissions.stream()
                .filter(p -> p.getOperation().equals(AuthorizationService.Operation.READ))
                .map(Permission::getAuthorizationAttribute)
                .collect(Collectors.toList());
        final List<AuthorizationAttribute> writers = permissions.stream()
                .filter(p -> p.getOperation().equals(AuthorizationService.Operation.WRITE))
                .map(Permission::getAuthorizationAttribute)
                .collect(Collectors.toList());

        return new ResourceAuthorization(admins, readers, writers);
    }

    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        switch (operation) {
            case READ:
                return Optional.of(getReaders());
            case WRITE:
                return Optional.of(getWriters());
            case ADMIN:
                return Optional.of(getAdmins());
            case VIEW:
                return Optional.of(getAll());
            default:
                throw new IllegalArgumentException("Operation " + operation + " is not supported");
        }
    }


    @Override
    public Map<String, List<AuthorizationAttribute>> asMapValue() {
        return ImmutableMap.of(
                "admins", getAdmins(),
                "readers", getReaders(),
                "writers", getWriters());
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }

        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ResourceAuthorization that = (ResourceAuthorization) o;
        return Objects.equals(admins, that.admins) &&
                Objects.equals(readers, that.readers) &&
                Objects.equals(writers, that.writers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(admins, readers, writers);
    }
}
