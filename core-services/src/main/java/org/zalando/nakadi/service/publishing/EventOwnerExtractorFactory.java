package org.zalando.nakadi.service.publishing;

import com.google.common.annotations.VisibleForTesting;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.exceptions.runtime.JsonPathAccessException;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.util.JsonPathAccess;
import org.zalando.nakadi.view.EventOwnerSelector;

import java.util.function.Function;

@Component
public class EventOwnerExtractorFactory {

    private final FeatureToggleService featureToggleService;

    @Autowired
    public EventOwnerExtractorFactory(final FeatureToggleService featureToggleService) {
        this.featureToggleService = featureToggleService;
    }

    public Function<JSONObject, EventOwnerHeader> createExtractor(final EventType eventType) {
        final EventOwnerSelector selector = eventType.getEventOwnerSelector();
        if (null == selector || !(featureToggleService.isFeatureEnabled(Feature.EVENT_OWNER_SELECTOR_AUTHZ))) {
            return null;
        }
        switch (selector.getType()) {
            case PATH:
                return createPathExtractor(selector);
            case STATIC:
                return createStaticExtractor(selector);
            default:
                throw new IllegalArgumentException("Unsupported Type for event_owner_selector: " + selector.getType());
        }
    }

    @VisibleForTesting
    static Function<JSONObject, EventOwnerHeader> createPathExtractor(final EventOwnerSelector selector) {
        return (batchItem) -> {
            try {
                final JsonPathAccess jsonPath = new JsonPathAccess(batchItem);
                final Object value = jsonPath.get(selector.getValue());
                return JSONObject.NULL == value ? null : new EventOwnerHeader(selector.getName(), value.toString());
            } catch (final JsonPathAccessException e) {
                return null;
            }
        };
    }

    @VisibleForTesting
    static Function<JSONObject, EventOwnerHeader> createStaticExtractor(final EventOwnerSelector selector) {
        final EventOwnerHeader eventOwnerHeader = new EventOwnerHeader(selector.getName(), selector.getValue());
        return batchItem -> eventOwnerHeader;
    }
}
