package de.zalando.aruha.nakadi.domain;

import com.google.common.collect.ImmutableList;

import java.util.Collections;
import java.util.List;

public class Topology {

    private final Long version;

    private final List<ClientTopology> distribution;

    public Topology(final Long version, final List<ClientTopology> distribution) {
        this.version = version;
        this.distribution = ImmutableList.copyOf(distribution);
    }

    public Long getVersion() {
        return version;
    }

    public List<ClientTopology> getDistribution() {
        return Collections.unmodifiableList(distribution);
    }
}
