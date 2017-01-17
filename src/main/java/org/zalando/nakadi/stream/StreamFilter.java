package org.zalando.nakadi.stream;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public final class StreamFilter {

    @NotNull
    private Set<String> eventTypes;
    @NotNull
    private List<String> expressions;

    public List<String> getExpressions() {
        return expressions;
    }

    public void setExpressions(final List<String> expressions) {
        this.expressions = Collections.unmodifiableList(expressions);
    }

    public Set<String> getEventTypes() {
        return eventTypes;
    }

    public void setEventTypes(final Set<String> eventTypes) {
        this.eventTypes = Collections.unmodifiableSet(eventTypes);
    }

}
