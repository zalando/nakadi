package org.zalando.nakadi.validation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ValidationStrategyConfiguration;

import java.util.Map;

public abstract class ValidationStrategy {

    private static final Map<String, ValidationStrategy> STRATEGIES = Maps.newHashMap();

    public abstract EventValidator materialize(EventType eventType, ValidationStrategyConfiguration vsc);

    static {
        register(EventBodyMustRespectSchema.NAME, new EventBodyMustRespectSchema(new JsonSchemaEnrichment()));
        register(EventMetadataValidationStrategy.NAME, new EventMetadataValidationStrategy());
    }

    public static final void register(final String name, final ValidationStrategy strategy) {
        STRATEGIES.put(name, strategy);
    }

    public static final ValidationStrategy lookup(final String strategyName) {
        Preconditions.checkArgument(STRATEGIES.containsKey(strategyName), "No such strategy %s", strategyName);
        return STRATEGIES.get(strategyName);
    }
}
