package org.zalando.nakadi.service;

import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.FeatureWrapper;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
public interface FeatureToggleService {

    void setFeature(FeatureWrapper feature);

    boolean isFeatureEnabled(Feature feature);

    default List<FeatureWrapper> getFeatures() {
        return Arrays.stream(Feature.values())
                .map(feature -> new FeatureWrapper(feature, isFeatureEnabled(feature)))
                .collect(Collectors.toList());
    }

}
