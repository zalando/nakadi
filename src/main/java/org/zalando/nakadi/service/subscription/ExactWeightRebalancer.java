package org.zalando.nakadi.service.subscription;

import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
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
        final Map<String, Integer> activeSessionWeights = Stream.of(sessions)
                .collect(Collectors.toMap(Session::getId, Session::getWeight));
        // sorted session ids.
        final List<String> activeSessionIds = activeSessionWeights.keySet().stream().sorted()
                .collect(Collectors.toList());
        // the main part of rebalance - calculate count for each partition.
        final int[] partitionsPerSession = splitByWeight(
                currentPartitions.length,
                activeSessionIds.stream().mapToInt(activeSessionWeights::get).toArray());

        // Stage 1. Select partitions that are not assigned to any EXISTING session.
        final Set<Partition> toRebalance = Stream.of(currentPartitions)
                .filter(p -> p.mustBeRebalanced(activeSessionIds))
                .collect(Collectors.toSet());

        // Stage 2. Remove partitions from sessions that have too many of them.
        // 2.1. collect information per session.
        final Map<String, List<Partition>> partitions = Stream.of(currentPartitions)
                .filter(p -> !toRebalance.contains(p))
                .collect(Collectors.groupingBy(Partition::getSessionOrNextSession));
        // 2.2. Remove
        for (int idx = 0; idx < activeSessionIds.size(); ++idx) {
            final String sessionId = activeSessionIds.get(idx);
            final int suggestedCount = partitionsPerSession[idx];
            int toTake = (partitions.containsKey(sessionId) ? partitions.get(sessionId).size() : 0) - suggestedCount;
            while (toTake > 0) {
                final List<Partition> candidates = partitions.get(sessionId);
                final Partition toTakeItem = candidates.stream()
                        .filter(p -> p.getState() == Partition.State.REASSIGNING).findAny().orElse(
                        candidates.get(candidates.size() - 1));
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
    /**
     * Proportionally split an itemCount based on array of weights. Sum of the result array must be equal to itemCount.
     * For example, a count 101 with weights [1, 1, 2] can be splitted to [25, 25, 51]
     */
    static int[] splitByWeight(final int itemCount, final int[] weigths) {
        if (itemCount < weigths.length || weigths.length == 0) {
            throw new IllegalArgumentException("Can not rebalance " + itemCount + " onto " + weigths.length);
        }
        if (IntStream.of(weigths).filter(w -> w <= 0).findAny().isPresent()) {
            throw new IllegalArgumentException("Weight can not be below zero: " + Arrays.toString(weigths));
        }
        final int totalWeight = IntStream.of(weigths).sum();
        final int fixed = itemCount / totalWeight;
        final int size = weigths.length;
        final int[] result = new int[size];
        if (fixed == 0) {
            Arrays.fill(result, 1);
            return result;
        } else if (itemCount == totalWeight) {
            return weigths;
        } else {
            double leftOver = 0.0;
            for (int i = 0; i < size; i++) {
                final double weightedValue = weigths[i] * itemCount * 1.0 / totalWeight + leftOver;
                result[i] = (int) weightedValue;
                leftOver = weightedValue - result[i];
            }
            result[size - 1] = (int) (result[size - 1] + leftOver);
            return result;
        }
    }

}
