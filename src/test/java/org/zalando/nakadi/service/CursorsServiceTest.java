package org.zalando.nakadi.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.zalando.nakadi.domain.Cursor;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperLockFactory;
import org.zalando.nakadi.service.subscription.KafkaClient;
import org.zalando.nakadi.service.subscription.SubscriptionKafkaClientFactory;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClientFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static java.text.MessageFormat.format;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CursorsServiceTest {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    private static final String SID = "sid";
    private static final String MY_ET = "my-et";
    private static final String NEW_OFFSET = "newOffset";
    private static final List<Cursor> DUMMY_CURSORS = ImmutableList.of(new Cursor("p1", NEW_OFFSET));

    private TopicRepository topicRepository;
    private CursorsService cursorsService;
    private GetDataBuilder getDataBuilder;
    private SetDataBuilder setDataBuilder;
    private ZkSubscriptionClient zkSubscriptionClient;
    private KafkaClient kafkaClient;
    private GetChildrenBuilder getChildrenBuilder;

    @Before
    public void before() throws Exception {
        final CuratorFramework curatorFramework = mock(CuratorFramework.class);
        getDataBuilder = mock(GetDataBuilder.class);
        when(curatorFramework.getData()).thenReturn(getDataBuilder);
        setDataBuilder = mock(SetDataBuilder.class);
        when(curatorFramework.setData()).thenReturn(setDataBuilder);
        getChildrenBuilder = mock(GetChildrenBuilder.class);
        when(curatorFramework.getChildren()).thenReturn(getChildrenBuilder);

        final ZooKeeperHolder zkHolder = mock(ZooKeeperHolder.class);
        when(zkHolder.get()).thenReturn(curatorFramework);

        topicRepository = mock(TopicRepository.class);

        final ZooKeeperLockFactory zkLockFactory = mock(ZooKeeperLockFactory.class);
        final InterProcessLock lock = mock(InterProcessLock.class);
        when(zkLockFactory.createLock(any())).thenReturn(lock);

        final SubscriptionDbRepository subscriptionRepository = mock(SubscriptionDbRepository.class);
        final Subscription subscription = new Subscription();
        subscription.setEventTypes(ImmutableSet.of(MY_ET));
        when(subscriptionRepository.getSubscription(any())).thenReturn(subscription);

        final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
        final EventType eventType = buildDefaultEventType();
        eventType.setTopic(MY_ET);
        when(eventTypeRepository.findByName(MY_ET)).thenReturn(eventType);

        final ZkSubscriptionClientFactory zkSubscriptionClientFactory = mock(ZkSubscriptionClientFactory.class);
        zkSubscriptionClient = mock(ZkSubscriptionClient.class);
        when(zkSubscriptionClient.isSubscriptionCreated()).thenReturn(true);
        when(zkSubscriptionClientFactory.createZkSubscriptionClient(any())).thenReturn(zkSubscriptionClient);

        final SubscriptionKafkaClientFactory subscriptionKafkaClientFactory = mock(SubscriptionKafkaClientFactory.class);
        kafkaClient = mock(KafkaClient.class);
        when(subscriptionKafkaClientFactory.createKafkaClient(any())).thenReturn(kafkaClient);

        cursorsService = new CursorsService(zkHolder, topicRepository, subscriptionRepository,
                eventTypeRepository, zkLockFactory, zkSubscriptionClientFactory, subscriptionKafkaClientFactory);
    }

    @Test
    public void whenCommitCursorsThenTrue() throws Exception {
        when(getDataBuilder.forPath(any())).thenReturn("oldOffset".getBytes(CHARSET));
        when(topicRepository.compareOffsets(NEW_OFFSET, "oldOffset")).thenReturn(1);

        final boolean committed = cursorsService.commitCursors(SID, DUMMY_CURSORS);

        assertThat(committed, is(true));
        verify(setDataBuilder, times(1)).forPath(eq(offsetPath("p1")), eq("newOffset".getBytes(CHARSET)));
    }

    @Test
    public void whenFirstCursorIsNotCommittedThenNextCursorsAreNotSkipped() throws Exception {
        when(getDataBuilder.forPath(offsetPath("p1"))).thenReturn("p1currentOffset".getBytes(CHARSET));
        when(getDataBuilder.forPath(offsetPath("p2"))).thenReturn("p2currentOffset".getBytes(CHARSET));

        when(topicRepository.compareOffsets("p1offset", "p1currentOffset")).thenReturn(-1);
        when(topicRepository.compareOffsets("p2offset", "p2currentOffset")).thenReturn(1);

        final ImmutableList<Cursor> cursors = ImmutableList.of(
                new Cursor("p1", "p1offset"), new Cursor("p2", "p2offset"));

        final boolean committed = cursorsService.commitCursors(SID, cursors);

        assertThat(committed, is(false));
        verify(setDataBuilder, times(1)).forPath(eq(offsetPath("p2")), eq("p2offset".getBytes(CHARSET)));
    }

    @Test
    public void whenCommitOldCursorsThenFalse() throws Exception {
        when(getDataBuilder.forPath(any())).thenReturn("oldOffset".getBytes(CHARSET));
        when(topicRepository.compareOffsets(NEW_OFFSET, "oldOffset")).thenReturn(-1);

        final boolean committed = cursorsService.commitCursors(SID, DUMMY_CURSORS);

        assertThat(committed, is(false));
        verify(setDataBuilder, never()).forPath(any(), any());
    }

    @Test(expected = ServiceUnavailableException.class)
    public void whenExceptionThenServiceUnavailableException() throws Exception {
        when(getDataBuilder.forPath(any())).thenThrow(new Exception());
        cursorsService.commitCursors(SID, DUMMY_CURSORS);
    }

    @Test
    public void whenSubscriptionInZkNotCreatedThenCreate() throws Exception {
        when(getDataBuilder.forPath(any())).thenReturn("oldOffset".getBytes(CHARSET));
        when(topicRepository.compareOffsets(NEW_OFFSET, "oldOffset")).thenReturn(1);

        when(zkSubscriptionClient.isSubscriptionCreated()).thenReturn(false);
        doAnswer(invocation -> {
            final Runnable runnable = (Runnable) invocation.getArguments()[0];
            runnable.run();
            return null;
        }).when(zkSubscriptionClient).runLocked(any());
        when(zkSubscriptionClient.createSubscription()).thenReturn(true);

        final ImmutableMap<Partition.PartitionKey, Long> offsets = ImmutableMap.of();
        when(kafkaClient.getSubscriptionOffsets()).thenReturn(offsets);

        final boolean committed = cursorsService.commitCursors(SID, DUMMY_CURSORS);
        assertThat(committed, is(true));
        verify(zkSubscriptionClient, times(1)).fillEmptySubscription(eq(offsets));
    }

    @Test
    public void whenGetSubscriptionCursorsThenList() throws Exception {
        final String partition1 = "partition-1";
        final String partition2 = "partition-2";
        final String offset1 = "offset-1";
        final String offset2 = "offset-2";
        final List<String> partitions = Arrays.asList(partition1, partition2);

        when(getChildrenBuilder.forPath(any())).thenReturn(partitions);
        when(getDataBuilder.forPath(offsetPath(partition1))).thenReturn(offset1.getBytes(CHARSET));
        when(getDataBuilder.forPath(offsetPath(partition2))).thenReturn(offset2.getBytes(CHARSET));

        final List<Cursor> actualResult = cursorsService.getSubscriptionCursors(SID);
        Assert.assertEquals(Arrays.asList(new Cursor(partition1, offset1), new Cursor(partition2, offset2)), actualResult);
    }

    @Test(expected = ServiceUnavailableException.class)
    public void whenGetSubscriptionCursorsExceptionThenServiceUnavailableException() throws Exception {
        when(getChildrenBuilder.forPath(any())).thenThrow(new Exception());
        cursorsService.getSubscriptionCursors(SID);
    }

    @Test(expected = ServiceUnavailableException.class)
    public void whenGetSubscriptionCursorsRuntimeExceptionThenServiceUnavailableException() throws Exception {
        when(getDataBuilder.forPath(any())).thenThrow(new Exception());
        cursorsService.getSubscriptionCursors(SID);
    }

    private String offsetPath(final String partition) {
        return format("/nakadi/subscriptions/{0}/topics/{1}/{2}/offset", SID, MY_ET, partition);
    }
}
