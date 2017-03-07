package org.zalando.nakadi.validation.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CompatibilityModeChangeConstraint implements SchemaEvolutionConstraint {
    final Map<CompatibilityMode, List<CompatibilityMode>> allowedChanges = ImmutableMap.of(
            CompatibilityMode.COMPATIBLE, Lists.newArrayList(CompatibilityMode.COMPATIBLE),
            CompatibilityMode.FORWARD, Lists.newArrayList(CompatibilityMode.FORWARD, CompatibilityMode.COMPATIBLE),
            CompatibilityMode.NONE, Lists.newArrayList(CompatibilityMode.NONE, CompatibilityMode.FORWARD)
    );

    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        if (!allowedChanges.get(original.getCompatibilityMode()).contains(eventType.getCompatibilityMode())) {
            return Optional.of(new SchemaEvolutionIncompatibility("changing compatibility_mode is not allowed"));
        } else {
            return Optional.empty();
        }
    }
}
