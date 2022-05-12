package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NakadiRecordResource implements Resource<NakadiRecord> {
    private final NakadiRecord nakadiRecord;

    public NakadiRecordResource(final NakadiRecord nakadiRecord) {
        this.nakadiRecord = nakadiRecord;
    }

    @Override
    public String getName() {
        return nakadiRecord.getMetadata().getEid();
    }

    @Override
    public String getType() {
        return ResourceImpl.EVENT_RESOURCE;
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        if (operation == AuthorizationService.Operation.WRITE) {
            return Optional.ofNullable(nakadiRecord.getOwner())
                    .map(AuthorizationAttributeProxy::new)
                    .map(Collections::singletonList);
        }
        return Optional.empty();
    }

    @Override
    public NakadiRecord get() {
        return nakadiRecord;
    }

    @Override
    public Map<String, List<AuthorizationAttribute>> getAuthorization() {
        return null;
    }
}
