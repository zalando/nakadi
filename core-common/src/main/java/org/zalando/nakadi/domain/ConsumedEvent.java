package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Immutable
public class ConsumedEvent implements Resource<ConsumedEvent> {

    private final byte[] event;
    private final NakadiCursor position;
    private final long timestamp;
    private final EventAuthField eventAuthField;

    public ConsumedEvent(final byte[] event, final NakadiCursor position, final long timestamp,
                         @Nullable final EventAuthField eventAuthField) {
        this.event = event;
        this.position = position;
        this.timestamp = timestamp;
        this.eventAuthField = eventAuthField;
    }

    public byte[] getEvent() {
        return event;
    }

    public NakadiCursor getPosition() {
        return position;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ConsumedEvent)) {
            return false;
        }

        final ConsumedEvent that = (ConsumedEvent) o;
        return Objects.equals(this.event, that.event)
                && Objects.equals(this.position, that.position);
    }

    @Override
    public int hashCode() {
        return position.hashCode();
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public String getType() {
        return null;
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(AuthorizationService.Operation operation) {
        if (operation == AuthorizationService.Operation.READ) {
            return Optional.ofNullable(this.eventAuthField)
                    .map(ConsumedEvent::authToAttribute)
                    .map(Collections::singletonList);
        }
        return Optional.empty();
    }

    @Override
    public ConsumedEvent get() {
        return this;
    }

    @Override
    public Map<String, List<AuthorizationAttribute>> getAuthorization() {
        return Collections.emptyMap();
    }


    // TODO: Use the one from publishing stack, remove this useless method.
    public static AuthorizationAttribute authToAttribute(final EventAuthField auth) {
        return new AuthorizationAttribute() {
            @Override
            public String getDataType() {
                return auth.getName();
            }

            @Override
            public String getValue() {
                return auth.getValue();
            }
        };
    }
}
