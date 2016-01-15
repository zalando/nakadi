package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.SubscriptionRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RegularPartitionDistributorTest {

    /**
     * Test checks if partitions of three topics are distributed in a following way among three clients:
     * topic "a"
     *   partition 0 - client 0
     *   partition 1 - client 1
     *   partition 2 - client 2
     *   partition 3 - client 0
     *   partition 4 - client 1
     *   partition 5 - client 2
     *   partition 6 - client 0
     *   partition 7 - client 1
     * topic "b"
     *   partition 0 - client 2
     *   partition 1 - client 0
     * topic "c"
     *   partition 0 - client 1
     *   partition 1 - client 2
     *   partition 2 - client 0
     *   partition 3 - client 1
     */
    @Test
    public void whenDistributeMultipleTopicPartitionsForThreeClientsThenOk() {

        // ARRANGE //
        final String subscriptionId = "sub1";
        final SubscriptionRepository subscriptionRepository =
                subscriptionRepositoryMock(subscriptionId, ImmutableList.of("c", "a", "b"));

        final TopicRepository topicRepository = mock(TopicRepository.class);
        when(topicRepository.listPartitions("a")).thenReturn(ImmutableList.of("3", "4", "0", "1", "5", "2", "6", "7"));
        when(topicRepository.listPartitions("b")).thenReturn(ImmutableList.of("0", "1"));
        when(topicRepository.listPartitions("c")).thenReturn(ImmutableList.of("3", "1", "2", "0"));

        // ACT //
        final RegularPartitionDistributor distributor =
                new RegularPartitionDistributor(topicRepository, subscriptionRepository);
        final Map<Integer, List<TopicPartition>> partitionsForClients =
                distributor.getPartitionsForClients(subscriptionId, ImmutableList.of(0, 1, 2), 3);

        // ASSERT //
        assertThat(partitionsForClients, Matchers.equalTo(ImmutableMap.of(
                0, ImmutableList.of(
                        new TopicPartition("a", "0"),
                        new TopicPartition("a", "3"),
                        new TopicPartition("a", "6"),
                        new TopicPartition("b", "1"),
                        new TopicPartition("c", "2")),
                1, ImmutableList.of(
                        new TopicPartition("a", "1"),
                        new TopicPartition("a", "4"),
                        new TopicPartition("a", "7"),
                        new TopicPartition("c", "0"),
                        new TopicPartition("c", "3")),
                2, ImmutableList.of(
                        new TopicPartition("a", "2"),
                        new TopicPartition("a", "5"),
                        new TopicPartition("b", "0"),
                        new TopicPartition("c", "1"))
        )));
    }

    /**
     * Test checks if partitions of three topics are distributed in a following way among three clients:
     * topic "a"
     *   partition 0 - client 0
     *   partition 1 - client 1
     *   nothing - client 2
     */
    @Test
    public void whenDistributeLessPartitionsThanNumberOfClientsThenSomeClientsDontGetAnything() {

        // ARRANGE //
        final String subscriptionId = "sub1";
        final SubscriptionRepository subscriptionRepository =
                subscriptionRepositoryMock(subscriptionId, ImmutableList.of("a"));

        final TopicRepository topicRepository = mock(TopicRepository.class);
        when(topicRepository.listPartitions("a")).thenReturn(ImmutableList.of("0", "1"));

        // ACT //
        final RegularPartitionDistributor distributor =
                new RegularPartitionDistributor(topicRepository, subscriptionRepository);
        final Map<Integer, List<TopicPartition>> partitionsForClients =
                distributor.getPartitionsForClients(subscriptionId, ImmutableList.of(0, 1, 2), 3);

        // ASSERT //
        assertThat(partitionsForClients, Matchers.equalTo(ImmutableMap.of(
                0, ImmutableList.of(new TopicPartition("a", "0")),
                1, ImmutableList.of(new TopicPartition("a", "1")),
                2, ImmutableList.of()
        )));
    }

    private SubscriptionRepository subscriptionRepositoryMock(final String subscriptionId,
                                                              final ImmutableList<String> subscriptionTopic) {
        final SubscriptionRepository subscriptionRepository = mock(SubscriptionRepository.class);
        final Subscription subscription = new Subscription(subscriptionId, subscriptionTopic);
        when(subscriptionRepository.getSubscription(subscriptionId)).thenReturn(subscription);
        return subscriptionRepository;
    }

}
