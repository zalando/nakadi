package de.zalando.aruha.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;

@Immutable
public class PartitionResolutionStrategy {

    @NotNull
    private String name;

    @Nullable
    private String doc;

    public PartitionResolutionStrategy(@JsonProperty("name") final String name,
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
}
