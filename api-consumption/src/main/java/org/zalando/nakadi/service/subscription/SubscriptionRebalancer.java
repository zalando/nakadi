package org.zalando.nakadi.service.subscription;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class SubscriptionRebalancer implements BiFunction<Collection<Session>, Partition[], Partition[]> {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionRebalancer.class);

    @Override
    public Partition[] apply(final Collection<Session> sessions, final Partition[] currentPartitions) {

        final List<String> activeSessions = sessions.stream()
                .map(Session::getId)
                .collect(Collectors.toList());
        final List<Partition> partitionsLeft = Lists.newArrayList(currentPartitions);
        final List<Partition> changedPartitions = new ArrayList<>();

        final List<Session> sessionsWithSpecifiedPartitions = sessions.stream()
                .filter(s -> !s.getRequestedPartitions().isEmpty())
                .collect(Collectors.toList());

        // go through all sessions that directly requested partitions to stream
        for (final Session session : sessionsWithSpecifiedPartitions) {
            for (final EventTypePartition requestedPartition : session.getRequestedPartitions()) {

                // find a partition that is requested and assign it to a session that requests it
                final Optional<Partition> partitionOpt = partitionsLeft.stream()
                        .filter(p -> p.getKey().equals(requestedPartition))
                        .findFirst();
                if (!partitionOpt.isPresent()) {
                    LOG.warn("Two existing sessions request the same partition: " + requestedPartition);
                    return new Partition[0];
                }

                final Partition partition = partitionOpt.get();
                partitionsLeft.remove(partition);

                // if this partition is not assigned to this session - move it
                if (!session.getId().equals(partition.getSession())) {
                    final Partition movedPartition = partition.moveToSessionId(session.getId(), activeSessions);
                    changedPartitions.add(movedPartition);
                }
            }
        }

        // for the rest of partitions/sessions perform a rebalance based on partitions count
        final List<Session> autoBalanceSessions = sessions.stream()
                .filter(s -> s.getRequestedPartitions().isEmpty())
                .collect(Collectors.toList());

        if (!autoBalanceSessions.isEmpty() && !partitionsLeft.isEmpty()) {
            final Partition[] partitionsChangedByAutoRebalance = rebalanceByWeight(
                    autoBalanceSessions,
                    partitionsLeft.toArray(new Partition[partitionsLeft.size()]));
            changedPartitions.addAll(Arrays.asList(partitionsChangedByAutoRebalance));
        }
        return changedPartitions.toArray(new Partition[changedPartitions.size()]);
    }

    private Partition[] rebalanceByWeight(final Collection<Session> sessions, final Partition[] currentPartitions) {
        final Map<String, Integer> activeSessionWeights = sessions.stream()
                .collect(Collectors.toMap(Session::getId, Session::getWeight));
        // sorted session ids.
        final List<String> activeSessionIds = activeSessionWeights.keySet().stream().sorted()
                .collect(Collectors.toList());
        // the main part of rebalance - calculate count for each partition.
        final int[] partitionsPerSession = splitByWeight(
                currentPartitions.length,
                activeSessionIds.stream().mapToInt(activeSessionWeights::get).toArray());

        // Stage 1. Select partitions that are not assigned to any EXISTING session.
        final List<Partition> toRebalance = Stream.of(currentPartitions)
                .filter(p -> p.mustBeRebalanced(activeSessionIds))
                .collect(Collectors.toList());

        // State 2. Remove partitions from sessions that have too many of them.
        // 2.1. collect information per session.
        final Map<String, List<Partition>> partitions = Stream.of(currentPartitions)
                .filter(p -> !toRebalance.contains(p))
                .collect(Collectors.groupingBy(Partition::getEffectiveSession));
        // 2.2. Remove
        for (int idx = 0; idx < activeSessionIds.size(); ++idx) {
            final String sessionId = activeSessionIds.get(idx);
            final int suggestedCount = partitionsPerSession[idx];
            int toTake = (partitions.containsKey(sessionId) ? partitions.get(sessionId).size() : 0) - suggestedCount;
            while (toTake > 0) {
                final List<Partition> candidates = partitions.get(sessionId);
                final Partition toTakeItem = candidates.stream()
                        .filter(p -> p.getState() == Partition.State.REASSIGNING)
                        .findAny()
                        .orElse(candidates.get(candidates.size() - 1));
                candidates.remove(toTakeItem);
                toRebalance.add(toTakeItem);
                toTake -= 1;
            }
        }

        if (!toRebalance.isEmpty()) {
            // 3. Assign partitions to any nodes who are waiting for it.
            final List<Partition> result = new ArrayList<>();
            for (int idx = 0; idx < activeSessionIds.size(); ++idx) {
                final String sessionId = activeSessionIds.get(idx);

                final int suggestedCount = partitionsPerSession[idx];
                final int currentCount = partitions.containsKey(sessionId) ? partitions.get(sessionId).size() : 0;
                for (int i = 0; i < suggestedCount - currentCount; ++i) {
                    final Partition toMove = toRebalance.iterator().next();
                    toRebalance.remove(toMove);
                    result.add(toMove.moveToSessionId(sessionId, activeSessionIds));
                }
            }
            return result.toArray(new Partition[result.size()]);
        } else {
            return new Partition[0];
        }
    }

    static int[] splitByWeight(final int itemCount, final int[] weigths) {
        if (itemCount < weigths.length) {
            throw new IllegalArgumentException("Can not rebalance " + itemCount + " onto " + weigths.length);
        }
        if (IntStream.of(weigths).filter(w -> w <= 0).findAny().isPresent()) {
            throw new IllegalArgumentException("Weight can not be below zero: " + Arrays.toString(weigths));
        }
        final int totalWeight = IntStream.of(weigths).sum();
        final int fixed = itemCount / totalWeight;
        final int[] result = IntStream.of(weigths).map(w -> fixed * w).toArray();
        if (fixed == 0) {
            Arrays.fill(result, 1);
        }

        int left = itemCount - IntStream.of(result).sum();

        // Yes, it's bad way, I know. But failed to figure out other simple way for exact case.
        while (left > 0) {
            for (int i = 0; i < result.length && left > 0; ++i) {
                final int v = Math.min(left, weigths[i]);
                result[i] += v;
                left -= v;
            }
        }
        return result;
    }

}
