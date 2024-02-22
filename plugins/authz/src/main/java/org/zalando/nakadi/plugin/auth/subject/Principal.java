package org.zalando.nakadi.plugin.auth.subject;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

public abstract class Principal implements Subject {
    private final String uid;

    private final Supplier<Set<String>> retailerIdsSupplier;
    private Set<String> cachedRetailerIds;

    protected Principal(final String uid, final Supplier<Set<String>> retailerIdsSupplier) {
        this.uid = uid;
        this.retailerIdsSupplier = retailerIdsSupplier;
    }

    public String getUid() {
        return uid;
    }

    public abstract boolean isExternal();

    public abstract boolean isAuthorized(String resourceType,
                                         AuthorizationService.Operation operation,
                                         Optional<List<AuthorizationAttribute>> attributes);

    /**
     * Returns set of allowed business partner ids, only in case if the principal is external ({@code #isExternal}).
     *
     * @return Non-null set external ids
     * @throws PluginException in case if Principal is not external and is not supporting business partner ids
     */
    public abstract Set<String> getBpids() throws PluginException;

    Set<String> getRetailerIdsToRead() throws PluginException {
        if (null == cachedRetailerIds) {
            cachedRetailerIds = retailerIdsSupplier.get();
        }
        return cachedRetailerIds;
    }

    protected boolean isPerEventOperationAllowed(
            final AuthorizationService.Operation operation,
            final AuthorizationAttribute attribute)
            throws PluginException {

        // FIXME: Move strings to a constant in plugin api (related to all the authorization attributes)
        if (!Objects.equals(attribute.getDataType(), "retailer_id")) {
            return false;
        }
        switch (operation) {
            case READ:
                final Set<String> retailerIds = getRetailerIdsToRead();
                return retailerIds.contains("*") || retailerIds.contains(attribute.getValue());
            case WRITE:
                return true;
            default:
                return false;
        }
    }
}
