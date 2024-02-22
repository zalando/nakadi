package org.zalando.nakadi.plugin.auth.subject;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public class UidSubject extends Principal {

    protected final String type;

    public UidSubject(final String uid,
                      final Supplier<Set<String>> retailerIdsSupplier,
                      final String type) {
        super(uid, retailerIdsSupplier);
        this.type = type;
    }

    public String getName() {
        return getUid();
    }

    public boolean isExternal() {
        return false;
    }

    @Override
    public Set<String> getBpids() throws PluginException {
        throw new PluginException("UidSubject " + getUid() + " of type " + type +
                " does not support business partners");
    }

    public boolean isAuthorized(
            final String resourceType,
            final AuthorizationService.Operation operation,
            final Optional<List<AuthorizationAttribute>> attributes) {

        if (operation == AuthorizationService.Operation.VIEW) {
            return true;
        }

        if (!attributes.isPresent() || attributes.get().isEmpty()) {
            return true;
        }

        for (final AuthorizationAttribute attribute : attributes.get()) {
            if (attribute.getDataType().equals("*") && attribute.getValue().equals("*")) {
                return true;
            }
            if (attribute.getDataType().equals(type) && attribute.getValue().equals(getUid())) {
                return true;
            }
            if (isPerEventOperationAllowed(operation, attribute)) {
                return true;
            }
        }
        return false;
    }
}
