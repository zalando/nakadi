package org.zalando.nakadi.domain;

import com.google.common.collect.ImmutableMap;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class BatchItemAuthorization implements ValidatableAuthorization {

    private final List<AuthorizationAttribute> readers;
    private final List<AuthorizationAttribute> writers;

    public BatchItemAuthorization(final List<AuthorizationAttribute> readers,
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
                throw new IllegalArgumentException("Operation: " + operation + " is not supported");
        }
    }

    @Override
    public Map<String, List<AuthorizationAttribute>> asMapValue() {
        return ImmutableMap.of(
                AuthorizationService.Operation.READ.toString(), getReaders(),
                AuthorizationService.Operation.WRITE.toString(), getWriters()
        );
    }

    public static BatchItemAuthorization fromHeader(final EventOwnerHeader header) {
        final AuthorizationAttribute attribute = new ResourceAuthorizationAttribute(
                header.getName(), header.getValue());
        return new BatchItemAuthorization(
                Collections.singletonList(attribute),
                Collections.singletonList(attribute)
        );
    }
}
