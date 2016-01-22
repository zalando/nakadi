package de.zalando.aruha.nakadi.domain;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Topology {

    private static final Logger LOG = LoggerFactory.getLogger(Topology.class);

    private final List<String> clientIds;

    public Topology(final List<String> clientIds) {
        this.clientIds = clientIds
                        .stream()
                        .sorted()
                        .collect(Collectors.toList());
    }

    public List<String> getClientIds() {
        return Collections.unmodifiableList(clientIds);
    }

    public Optional<Integer> getClientIndex(final String clientId) {
        try {
            return Optional.of(clientIds.indexOf(clientId));
        }
        catch (Exception e) {
            LOG.warn("Exception occurred when trying to find client index in topology", e);
            return Optional.empty();
        }
    }

    @Override
    public int hashCode() {
        return clientIds.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Topology)) {
            return false;
        }
        final Topology topology = (Topology) obj;
        return this.clientIds.equals(topology.getClientIds());
    }

}
