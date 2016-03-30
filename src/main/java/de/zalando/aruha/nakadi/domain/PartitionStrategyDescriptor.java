package de.zalando.aruha.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

@Immutable
public class PartitionStrategyDescriptor {

    @NotNull
    private String name;

    @Nullable
    private String doc;

    public PartitionStrategyDescriptor(@JsonProperty("name") final String name,
                                       @JsonProperty("doc") @Nullable final String doc) {
        this.name = name;
        this.doc = doc;
    }

    public String getName() {
        return name;
    }

    @Nullable
    public String getDoc() {
        return doc;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final PartitionStrategyDescriptor that = (PartitionStrategyDescriptor) o;
        return !(name != null ? !name.equals(that.name) : that.name != null);
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
