package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.db.SubscriptionDbRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperLockFactory;
import de.zalando.aruha.nakadi.service.subscription.KafkaClient;
import de.zalando.aruha.nakadi.service.subscription.SubscriptionKafkaClientFactory;
import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import de.zalando.aruha.nakadi.service.subscription.zk.ZkSubscriptionClient;
import de.zalando.aruha.nakadi.service.subscription.zk.ZkSubscriptionClientFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.Assert;

import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import static de.zalando.aruha.nakadi.utils.TestUtils.buildDefaultEventType;
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

public class CursorsCommitServiceTest {

    private static final Charset CHARSET = Charset.forName("UTF-8");

    private static final String SID = "sid";
    private static final String MY_ET = "my-et";
    private static final String NEW_OFFSET = "newOffset";
    private static final List<Cursor> DUMMY_CURSORS = ImmutableList.of(new Cursor("p1", NEW_OFFSET));

    private TopicRepository topicRepository;
    private CursorsCommitService cursorsCommitService;
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

        cursorsCommitService = new CursorsCommitService(zkHolder, topicRepository, subscriptionRepository,
                eventTypeRepository, zkLockFactory, zkSubscriptionClientFactory, subscriptionKafkaClientFactory);
    }

    @Test
    public void whenCommitCursorsThenTrue() throws Exception {
        when(getDataBuilder.forPath(any())).thenReturn("oldOffset".getBytes(CHARSET));
        when(topicRepository.compareOffsets(NEW_OFFSET, "oldOffset")).thenReturn(1);

        final boolean committed = cursorsCommitService.commitCursors(SID, DUMMY_CURSORS);

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

        final boolean committed = cursorsCommitService.commitCursors(SID, cursors);

        assertThat(committed, is(false));
        verify(setDataBuilder, times(1)).forPath(eq(offsetPath("p2")), eq("p2offset".getBytes(CHARSET)));
    }

    @Test
    public void whenCommitOldCursorsThenFalse() throws Exception {
        when(getDataBuilder.forPath(any())).thenReturn("oldOffset".getBytes(CHARSET));
        when(topicRepository.compareOffsets(NEW_OFFSET, "oldOffset")).thenReturn(-1);

        final boolean committed = cursorsCommitService.commitCursors(SID, DUMMY_CURSORS);

        assertThat(committed, is(false));
        verify(setDataBuilder, never()).forPath(any(), any());
    }

    @Test(expected = ServiceUnavailableException.class)
    public void whenExceptionThenServiceUnavailableException() throws Exception {
        when(getDataBuilder.forPath(any())).thenThrow(new Exception());
        cursorsCommitService.commitCursors(SID, DUMMY_CURSORS);
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

        final boolean committed = cursorsCommitService.commitCursors(SID, DUMMY_CURSORS);
        assertThat(committed, is(true));
        verify(zkSubscriptionClient, times(1)).fillEmptySubscription(eq(offsets));
    }

    @Test
    public void whenGetSubscriptionThenList() throws Exception {

        when(getChildrenBuilder.forPath(any())).thenReturn(Arrays.asList("partition-1", "partition-2"));
        when(getDataBuilder.forPath(offsetPath("partition-1"))).thenReturn("offset-1".getBytes(CHARSET));
        when(getDataBuilder.forPath(offsetPath("partition-2"))).thenReturn("offset-2".getBytes(CHARSET));

        final List<Cursor> actualResult = cursorsCommitService.getSubscriptionCursors(SID);
        Assert.notEmpty(actualResult);
    }

    private String offsetPath(final String partition) {
        return format("/nakadi/subscriptions/{0}/topics/{1}/{2}/offset", SID, MY_ET, partition);
    }
}
