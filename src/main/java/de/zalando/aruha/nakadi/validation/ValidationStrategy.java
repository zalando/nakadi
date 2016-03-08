package de.zalando.aruha.nakadi.validation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;

import java.util.Map;

public abstract class ValidationStrategy {

    private static final Map<String, ValidationStrategy> STRATEGIES = Maps.newHashMap();

    public abstract EventValidator materialize(EventType eventType, ValidationStrategyConfiguration vsc);

    public static final void register(final String name, final ValidationStrategy strategy) {
        STRATEGIES.put(name, strategy);
    }

    public static final ValidationStrategy lookup(final String strategyName) {
        Preconditions.checkArgument(STRATEGIES.containsKey(strategyName), "No such strategy {}", strategyName);
        return STRATEGIES.get(strategyName);
    }
}
