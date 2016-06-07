package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import de.zalando.aruha.nakadi.service.subscription.model.Session;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

class ExactWeightRebalancer implements BiFunction<Session[], Partition[], Partition[]> {
    @Override
    public Partition[] apply(final Session[] sessions, final Partition[] currentPartitions) {
        final Map<String, Session> activeClients = Stream.of(sessions).collect(Collectors.toMap(c -> c.id, c -> c));
        // sorted client ids.
        final List<String> activeClientIds = activeClients.keySet().stream().sorted().collect(Collectors.toList());
        // the main part of rebalance - calculate count for each partition.
        final int[] partitionsPerClient = splitByWeight(
                currentPartitions.length,
                activeClientIds.stream().mapToInt(c -> activeClients.get(c).weight).toArray());

        // Stage 1. Select partitions that are not assigned to any EXISTING client.
        final Set<Partition> unassignedPartitions = Stream.of(currentPartitions)
                .filter(p -> Partition.State.UNASSIGNED.equals(p.getState()) || !activeClients.containsKey(p.getSessionOrNextSession()) || !activeClients.containsKey(p.getSession()))
                .collect(Collectors.toSet());

        // State 2. Remove partitions from clients that have too many of them.
        // 2.1. collect information per client.
        final Map<String, List<Partition>> partitions = Stream.of(currentPartitions)
                .filter(p -> !unassignedPartitions.contains(p))
                .collect(Collectors.groupingBy(Partition::getSessionOrNextSession));
        // 2.2. Remove
        for (int idx = 0; idx < activeClientIds.size(); ++idx) {
            final String clientId = activeClientIds.get(idx);
            final int suggestedCount = partitionsPerClient[idx];
            int toTake = (partitions.containsKey(clientId) ? partitions.get(clientId).size() : 0) - suggestedCount;
            while (toTake > 0) {
                final List<Partition> candidates = partitions.get(clientId);
                final Partition toTakeItem = candidates.stream().filter(p -> p.getState() == Partition.State.REASSIGNING).findAny().orElse(
                        candidates.get(candidates.size() - 1));
                candidates.remove(toTakeItem);
                unassignedPartitions.add(toTakeItem);
                toTake -= 1;
            }
        }

        if (!unassignedPartitions.isEmpty()) {
            // 3. Assign partitions to any nodes who are waiting for it, calculate new hash.
            final List<Partition> result = new ArrayList<>();
            for (int idx = 0; idx < activeClientIds.size(); ++idx) {
                final String clientId = activeClientIds.get(idx);

                final int suggestedCount = partitionsPerClient[idx];
                final int currentCount = partitions.containsKey(clientId) ? partitions.get(clientId).size() : 0;
                for (int i = 0; i < suggestedCount - currentCount; ++i) {
                    final Partition toMove = unassignedPartitions.iterator().next();
                    unassignedPartitions.remove(toMove);

                    if (toMove.getState() == Partition.State.UNASSIGNED) {
                        result.add(toMove.toState(Partition.State.ASSIGNED, clientId, null));
                    } else if (toMove.getState() == Partition.State.ASSIGNED) {
                        if (toMove.getSession().equals(clientId)) {
                            result.add(toMove.toState(Partition.State.ASSIGNED, clientId, null));
                        } else {
                            if (activeClientIds.contains(toMove.getSession())) {
                                result.add(toMove.toState(Partition.State.REASSIGNING, toMove.getSession(), clientId));
                            } else {
                                result.add(toMove.toState(Partition.State.ASSIGNED, clientId, null));
                            }
                        }
                    } else { // ASSIGNED
                        if (activeClientIds.contains(toMove.getSession())) {
                            // Taking from existing client.
                            result.add(toMove.toState(Partition.State.REASSIGNING, toMove.getSession(), clientId));
                        } else {
                            // Client was dead, directly start streaming
                            result.add(toMove.toState(Partition.State.ASSIGNED, clientId, null));
                        }
                    }
                }
            }
            return result.toArray(new Partition[result.size()]);
        } else {
            return null;
        }
    }

    static int[] splitByWeight(final int itemCount, final int[] weigths) {
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
