package org.zalando.nakadi.util;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

public class NakadiCollectionUtilsTest {
    @Test
    public void testDifference() {
        final Set<Integer> oldSet = ImmutableSet.of(0, 1, 2, 3, 4);
        final Set<Integer> newSet = ImmutableSet.of(1, 3, 4, 5, 6);
        final NakadiCollectionUtils.Diff<Integer> diff = NakadiCollectionUtils.difference(oldSet, newSet);
        Assert.assertEquals(diff.getAdded(), ImmutableSet.of(5, 6));
        Assert.assertEquals(diff.getRemoved(), ImmutableSet.of(0, 2));
    }

    @Test
    public void testDifferenceNoDifference() {
        final Set<Integer> oldSet = ImmutableSet.of(0, 1, 2, 3, 4);
        final Set<Integer> newSet = ImmutableSet.of(4, 3, 2, 1, 0);
        final NakadiCollectionUtils.Diff<Integer> diff = NakadiCollectionUtils.difference(oldSet, newSet);
        Assert.assertTrue(diff.getAdded().isEmpty());
        Assert.assertTrue(diff.getRemoved().isEmpty());
    }
}