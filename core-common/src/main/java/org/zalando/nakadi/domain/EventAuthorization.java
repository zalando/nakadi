package org.zalando.nakadi.domain;

import com.google.common.collect.ImmutableMap;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class EventAuthorization implements ValidatableAuthorization {

    private final List<AuthorizationAttribute> readers;
    private final List<AuthorizationAttribute> writers;

    public EventAuthorization(final List<AuthorizationAttribute> readers,
                              final List<AuthorizationAttribute> writers) {
        this.readers = readers;
        this.writers = writers;
    }

    public List<AuthorizationAttribute> getReaders() {
        return readers;
    }

    public List<AuthorizationAttribute> getWriters() {
        return writers;
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        switch (operation) {
            case READ:
                return Optional.of(getReaders());
            case WRITE:
                return Optional.of(getWriters());
            default:
                // todo (ferbncode): Fix me
                throw new IllegalStateException();
        }
    }

    @Override
    public Map<String, List<AuthorizationAttribute>> asMapValue() {
        return ImmutableMap.of(
                AuthorizationService.Operation.READ.toString(), getReaders(),
                AuthorizationService.Operation.WRITE.toString(), getWriters()
        );
    }

    public static EventAuthorization fromAttribute(final AuthorizationAttribute attribute) {
        return new EventAuthorization(
                Collections.singletonList(attribute),
                Collections.singletonList(attribute)
        );
    }

}
