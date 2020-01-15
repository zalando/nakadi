package org.zalando.nakadi.service;

import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.FeatureWrapper;
import org.zalando.nakadi.exceptions.runtime.FeatureNotAvailableException;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
public interface FeatureToggleService {

    void setFeature(FeatureWrapper feature);

    boolean isFeatureEnabled(Feature feature);

    void setAuditLogPublisher(NakadiAuditLogPublisher auditLogPublisher);

    default void checkFeatureOn(final Feature feature) {
        if (!isFeatureEnabled(feature)) {
            throw new FeatureNotAvailableException("Feature " + feature + " is disabled", feature);
        }
    }

    default List<FeatureWrapper> getFeatures() {
        return Arrays.stream(Feature.values())
                .map(feature -> new FeatureWrapper(feature, isFeatureEnabled(feature)))
                .collect(Collectors.toList());
    }

}
