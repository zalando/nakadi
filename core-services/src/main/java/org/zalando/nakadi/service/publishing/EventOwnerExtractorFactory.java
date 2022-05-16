package org.zalando.nakadi.service.publishing;

import com.google.common.annotations.VisibleForTesting;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.runtime.JsonPathAccessException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.util.JsonPathAccess;
import org.zalando.nakadi.view.EventOwnerSelector;

@Component
public class EventOwnerExtractorFactory {

    private final FeatureToggleService featureToggleService;

    @Autowired
    public EventOwnerExtractorFactory(final FeatureToggleService featureToggleService) {
        this.featureToggleService = featureToggleService;
    }

    public EventOwnerExtractor createExtractor(final EventType eventType) {
        final EventOwnerSelector selector = eventType.getEventOwnerSelector();
        if (null == selector || !(featureToggleService.isFeatureEnabled(Feature.EVENT_OWNER_SELECTOR_AUTHZ))) {
            return null;
        }
        switch (selector.getType()) {
            case PATH:
                return createPathExtractor(selector.getName(), selector.getValue());
            case STATIC:
                return createStaticExtractor(selector.getName(), selector.getValue());
            case METADATA:
                // value is ignored on purpose, may be extended later
                return createMetadataExtractor(selector.getName());
            default:
                throw new IllegalArgumentException("Unsupported Type for event_owner_selector: " + selector.getType());
        }
    }

    @VisibleForTesting
    static EventOwnerExtractor createPathExtractor(final String name, final String path) {
        return new EventOwnerExtractor() {
            @Override
            public EventOwnerHeader extractEventOwner(final JSONObject event) {
                try {
                    final JsonPathAccess jsonPath = new JsonPathAccess(event);
                    final Object value = jsonPath.get(path);
                    return JSONObject.NULL == value ? null : new EventOwnerHeader(name, value.toString());
                } catch (final JsonPathAccessException e) {
                    return null;
                }
            }

            @Override
            public EventOwnerHeader extractEventOwner(final NakadiMetadata metadata) {
                throw new NakadiBaseException("not implemented");
            }
        };
    }

    @VisibleForTesting
    static EventOwnerExtractor createStaticExtractor(final String name, final String value) {
        final EventOwnerHeader eventOwnerHeader = new EventOwnerHeader(name, value);

        return new EventOwnerExtractor() {
            @Override
            public EventOwnerHeader extractEventOwner(final JSONObject event) {
                return eventOwnerHeader;
            }

            @Override
            public EventOwnerHeader extractEventOwner(final NakadiMetadata metadata) {
                return eventOwnerHeader;
            }
        };
    }

    @VisibleForTesting
    static EventOwnerExtractor createMetadataExtractor(final String name) {
        return new EventOwnerExtractor() {
            @Override
            public EventOwnerHeader extractEventOwner(final JSONObject event) {
                throw new NakadiBaseException("not implemented");
            }

            @Override
            public EventOwnerHeader extractEventOwner(final NakadiMetadata metadata) {
                return new EventOwnerHeader(name, metadata.getEventOwner());
            }
        };
    }
}
