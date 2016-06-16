package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
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
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;
import java.util.List;

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

    public static final Charset CHARSET = Charset.forName("UTF-8");
    public static final String NEW_OFFSET = "newOffset";
    public static final List<Cursor> DUMMY_CURSORS = ImmutableList.of(new Cursor("p1", NEW_OFFSET));

    private TopicRepository topicRepository;
    private CursorsCommitService cursorsCommitService;
    private GetDataBuilder getDataBuilder;
    private SetDataBuilder setDataBuilder;
    private ZkSubscriptionClient zkSubscriptionClient;
    private KafkaClient kafkaClient;

    @Before
    public void before() throws Exception {
        final CuratorFramework curatorFramework = mock(CuratorFramework.class);
        getDataBuilder = mock(GetDataBuilder.class);
        when(curatorFramework.getData()).thenReturn(getDataBuilder);
        setDataBuilder = mock(SetDataBuilder.class);
        when(curatorFramework.setData()).thenReturn(setDataBuilder);

        final ZooKeeperHolder zkHolder = mock(ZooKeeperHolder.class);
        when(zkHolder.get()).thenReturn(curatorFramework);

        topicRepository = mock(TopicRepository.class);

        final ZooKeeperLockFactory zkLockFactory = mock(ZooKeeperLockFactory.class);
        final InterProcessLock lock = mock(InterProcessLock.class);
        when(zkLockFactory.createLock(any())).thenReturn(lock);

        final SubscriptionDbRepository subscriptionRepository = mock(SubscriptionDbRepository.class);
        final Subscription subscription = new Subscription();
        subscription.setEventTypes(ImmutableSet.of("my-et"));
        when(subscriptionRepository.getSubscription(any())).thenReturn(subscription);

        final ZkSubscriptionClientFactory zkSubscriptionClientFactory = mock(ZkSubscriptionClientFactory.class);
        zkSubscriptionClient = mock(ZkSubscriptionClient.class);
        when(zkSubscriptionClient.isSubscriptionCreated()).thenReturn(true);
        when(zkSubscriptionClientFactory.createZkSubscriptionClient(any())).thenReturn(zkSubscriptionClient);

        final SubscriptionKafkaClientFactory subscriptionKafkaClientFactory = mock(SubscriptionKafkaClientFactory.class);
        kafkaClient = mock(KafkaClient.class);
        when(subscriptionKafkaClientFactory.createKafkaClient(any())).thenReturn(kafkaClient);

        cursorsCommitService = new CursorsCommitService(zkHolder, topicRepository, subscriptionRepository,
                zkLockFactory, zkSubscriptionClientFactory, subscriptionKafkaClientFactory);
    }

    @Test
    public void whenCommitCursorsThenTrue() throws Exception {
        when(getDataBuilder.forPath(any())).thenReturn("oldOffset".getBytes(CHARSET));
        when(topicRepository.compareOffsets(NEW_OFFSET, "oldOffset")).thenReturn(1);

        final boolean committed = cursorsCommitService.commitCursors("sid", DUMMY_CURSORS);

        assertThat(committed, is(true));
        verify(setDataBuilder, times(1))
                .forPath(eq("/nakadi/subscriptions/sid/topics/my-et/p1/offset"), eq("newOffset".getBytes(CHARSET)));
    }

    @Test
    public void whenCommitOldCursorsThenFalse() throws Exception {
        when(getDataBuilder.forPath(any())).thenReturn("oldOffset".getBytes(CHARSET));
        when(topicRepository.compareOffsets(NEW_OFFSET, "oldOffset")).thenReturn(-1);

        final boolean committed = cursorsCommitService.commitCursors("sid", DUMMY_CURSORS);

        assertThat(committed, is(false));
        verify(setDataBuilder, never()).forPath(any(), any());
    }

    @Test(expected = ServiceUnavailableException.class)
    public void whenExceptionThenServiceUnavailableException() throws Exception {
        when(getDataBuilder.forPath(any())).thenThrow(new Exception());
        cursorsCommitService.commitCursors("sid", DUMMY_CURSORS);
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

        final boolean committed = cursorsCommitService.commitCursors("sid", DUMMY_CURSORS);
        assertThat(committed, is(true));
        verify(zkSubscriptionClient, times(1)).fillEmptySubscription(eq(offsets));
    }
}
