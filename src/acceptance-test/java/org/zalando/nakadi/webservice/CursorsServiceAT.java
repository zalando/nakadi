package org.zalando.nakadi.webservice;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.curator.framework.CuratorFramework;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InvalidStreamIdException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.security.FullAccessClient;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorsService;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.NewZkSubscriptionClient;
import org.zalando.nakadi.service.subscription.zk.SubscriptionClientFactory;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.util.List;

import static com.google.common.base.Charsets.UTF_8;
import static java.text.MessageFormat.format;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.utils.TestUtils.createFakeTimeline;
import static org.zalando.nakadi.utils.TestUtils.randomUUID;
import static org.zalando.nakadi.utils.TestUtils.randomValidEventTypeName;

public class CursorsServiceAT extends BaseAT {

    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);
    private static final String SUBSCRIPTIONS_PATH = "/nakadi/subscriptions";

    private static final String NEW_OFFSET = "002_newOffset";
    private static final String OLD_OFFSET = "001_oldOffset";
    private static final String OLDEST_OFFSET = "000_oldestOffset";

    private static final Answer<Integer> FAKE_OFFSET_COMPARATOR = invocation -> {
        final NakadiCursor c1 = (NakadiCursor) invocation.getArguments()[0];
        final NakadiCursor c2 = (NakadiCursor) invocation.getArguments()[1];
        return c1.getOffset().compareTo(c2.getOffset());
    };
    private static final Client FULL_ACCESS_CLIENT = new FullAccessClient("nakadi");

    private static final String P1 = "p1";
    private static final String P2 = "p2";

    private CursorConverter cursorConverter;
    private CursorsService cursorsService;
    private EventTypeRepository eventTypeRepository;

    private String etName;
    private String topic;
    private String sid;
    private String streamId;
    private List<NakadiCursor> testCursors;
    private Timeline timeline;

    private void registerNakadiCursor(final NakadiCursor cursor) {
        try {
            final SubscriptionCursorWithoutToken view = new SubscriptionCursorWithoutToken(
                    cursor.getEventType(), cursor.getPartition(), cursor.getOffset());
            when(cursorConverter.convert(eq(view))).thenReturn(cursor);
            when(cursorConverter.convertToNoToken(eq(cursor))).thenReturn(view);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Before
    public void before() throws Exception {
        sid = randomUUID();
        streamId = randomUUID();
        etName = randomValidEventTypeName();
        topic = randomUUID();
        cursorConverter = mock(CursorConverter.class);
        testCursors = ImmutableList.of(new NakadiCursor(createFakeTimeline(etName, topic), P1, NEW_OFFSET));

        final EventType eventType = mock(EventType.class);
        when(eventType.getTopic()).thenReturn(topic);
        when(eventType.getName()).thenReturn(etName);

        eventTypeRepository = mock(EventTypeRepository.class);
        when(eventTypeRepository.findByName(etName)).thenReturn(eventType);

        final ZooKeeperHolder zkHolder = mock(ZooKeeperHolder.class);
        when(zkHolder.get()).thenReturn(CURATOR);

        final TopicRepository topicRepository = mock(TopicRepository.class);
        when(topicRepository.compareOffsets(any(), any())).thenAnswer(FAKE_OFFSET_COMPARATOR);
        final TimelineService timelineService = mock(TimelineService.class);
        when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepository);
        timeline = createFakeTimeline(etName, topic);
        when(timelineService.getTimeline(any())).thenReturn(timeline);

        final Subscription subscription = mock(Subscription.class);
        when(subscription.getId()).thenReturn(sid);
        when(subscription.getEventTypes()).thenReturn(ImmutableSet.of(etName));
        final SubscriptionDbRepository subscriptionRepo = mock(SubscriptionDbRepository.class);
        when(subscriptionRepo.getSubscription(sid)).thenReturn(subscription);
        final SubscriptionClientFactory zkSubscriptionFactory = new SubscriptionClientFactory(zkHolder, MAPPER);
        cursorsService = new CursorsService(timelineService, subscriptionRepo, eventTypeRepository,
                mock(NakadiSettings.class), zkSubscriptionFactory, cursorConverter);

        // Register cursors in converter
        registerNakadiCursor(new NakadiCursor(createFakeTimeline(etName, topic), P1, NEW_OFFSET));
        registerNakadiCursor(new NakadiCursor(createFakeTimeline(etName, topic), P1, OLD_OFFSET));
        registerNakadiCursor(new NakadiCursor(createFakeTimeline(etName, topic), P2, NEW_OFFSET));
        registerNakadiCursor(new NakadiCursor(createFakeTimeline(etName, topic), P2, OLD_OFFSET));
        // bootstrap data in ZK
        CURATOR.create().creatingParentsIfNeeded().forPath(offsetPath(P1), OLD_OFFSET.getBytes(UTF_8));
        CURATOR.create().creatingParentsIfNeeded().forPath(offsetPath(P2), OLD_OFFSET.getBytes(UTF_8));
        CURATOR.create().creatingParentsIfNeeded().forPath(sessionPath(streamId));
    }

    @After
    public void after() throws Exception {
        CURATOR.delete().deletingChildrenIfNeeded().forPath(subscriptionPath());
    }

    @Test
    public void whenCommitCursorsThenTrue() throws Exception {
        setPartitions(new Partition[]{new Partition(etName, P1, streamId, null, Partition.State.ASSIGNED)});
        final List<Boolean> commitResult = cursorsService.commitCursors(streamId, sid, testCursors, FULL_ACCESS_CLIENT);
        assertThat(commitResult, equalTo(ImmutableList.of(true)));
        checkCurrentOffsetInZk(P1, NEW_OFFSET);
    }

    @Test
    public void whenStreamIdInvalidThenException() throws Exception {
        try {
            cursorsService.commitCursors("wrong-stream-id", sid, testCursors, FULL_ACCESS_CLIENT);
            fail("Expected InvalidStreamIdException to be thrown");
        } catch (final InvalidStreamIdException ignore) {
        }
        checkCurrentOffsetInZk(P1, OLD_OFFSET);
    }

    @Test
    public void whenPartitionIsStreamedToDifferentClientThenFalse() throws Exception {
        setPartitions(new Partition[]{new Partition(etName, P1, "wrong-stream-id", null, Partition.State.ASSIGNED)});
        try {
            cursorsService.commitCursors(streamId, sid, testCursors, FULL_ACCESS_CLIENT);
            fail("Expected InvalidStreamIdException to be thrown");
        } catch (final InvalidStreamIdException ignore) {
        }
        checkCurrentOffsetInZk(P1, OLD_OFFSET);
    }

    @Test
    public void whenCommitOldCursorsThenFalse() throws Exception {
        final NakadiCursor cursor = new NakadiCursor(createFakeTimeline(etName, topic), P1, OLDEST_OFFSET);
        registerNakadiCursor(cursor);
        testCursors = ImmutableList.of(cursor);
        setPartitions(new Partition[]{new Partition(etName, P1, streamId, null, Partition.State.ASSIGNED)});
        final List<Boolean> commitResult = cursorsService.commitCursors(streamId, sid, testCursors, FULL_ACCESS_CLIENT);
        assertThat(commitResult, equalTo(ImmutableList.of(false)));
        checkCurrentOffsetInZk(P1, OLD_OFFSET);
    }

    @Test
    public void whenFirstCursorIsNotCommittedThenNextCursorsAreNotSkipped() throws Exception {
        final NakadiCursor c1 = new NakadiCursor(timeline, P1, OLDEST_OFFSET);
        final NakadiCursor c2 = new NakadiCursor(timeline, P2, NEW_OFFSET);
        testCursors = ImmutableList.of(c1, c2);
        testCursors.forEach(this::registerNakadiCursor);

        setPartitions(new Partition[]{
                new Partition(etName, P1, streamId, null, Partition.State.ASSIGNED),
                new Partition(etName, P2, streamId, null, Partition.State.ASSIGNED)});
        final List<Boolean> result = cursorsService.commitCursors(streamId, sid, testCursors, FULL_ACCESS_CLIENT);

        assertFalse(result.get(0));
        assertTrue(result.get(1));

        checkCurrentOffsetInZk(P1, OLD_OFFSET);
        checkCurrentOffsetInZk(P2, NEW_OFFSET);
    }

    @Test
    public void whenMultipleCursorsForSamePartitionThenResultsAreCorrect() throws Exception {
        setPartitions(
                new Partition[]{
                        new Partition(etName, P1, streamId, null, Partition.State.ASSIGNED),
                        new Partition(etName, P2, streamId, null, Partition.State.ASSIGNED)
                });
        CURATOR.setData().forPath(offsetPath(P1), "000000000000000100".getBytes(UTF_8));
        CURATOR.setData().forPath(offsetPath(P2), "000000000000000800".getBytes(UTF_8));
        registerNakadiCursor(new NakadiCursor(timeline, P1, "000000000000000100"));
        registerNakadiCursor(new NakadiCursor(timeline, P2, "000000000000000800"));
        testCursors = ImmutableList.of(
                new NakadiCursor(timeline, P1, "000000000000000105"),
                new NakadiCursor(timeline, P1, "000000000000000106"),
                new NakadiCursor(timeline, P1, "000000000000000102"),
                new NakadiCursor(timeline, P1, "000000000000000096"),
                new NakadiCursor(timeline, P1, "000000000000000130"),
                new NakadiCursor(timeline, P2, "000000000000000800"),
                new NakadiCursor(timeline, P2, "000000000000000820"),
                new NakadiCursor(timeline, P1, "000000000000000120"),
                new NakadiCursor(timeline, P1, "000000000000000121"),
                new NakadiCursor(timeline, P2, "000000000000000825")
        );
        testCursors.forEach(this::registerNakadiCursor);

        final List<Boolean> commitResult = cursorsService.commitCursors(streamId, sid, testCursors, FULL_ACCESS_CLIENT);
        assertThat(commitResult, equalTo(
                ImmutableList.of(
                        true,
                        true,
                        false,
                        false,
                        true,
                        false,
                        true,
                        false,
                        false,
                        true
                )));

        checkCurrentOffsetInZk(P1, "000000000000000130");
        checkCurrentOffsetInZk(P2, "000000000000000825");
    }


    @Test
    public void whenGetSubscriptionCursorsThenOk() throws Exception {
        setPartitions(new Partition[]{
                new Partition(etName, P1, "", "", Partition.State.UNASSIGNED),
                new Partition(etName, P2, "", "", Partition.State.UNASSIGNED)
        });
        final List<SubscriptionCursorWithoutToken> cursors = cursorsService.getSubscriptionCursors(
                sid, FULL_ACCESS_CLIENT);

        assertThat(ImmutableSet.copyOf(cursors),
                equalTo(ImmutableSet.of(
                        new SubscriptionCursorWithoutToken(etName, P1, OLD_OFFSET),
                        new SubscriptionCursorWithoutToken(etName, P2, OLD_OFFSET)
                )));
    }

    private void checkCurrentOffsetInZk(final String partition, final String offset) throws Exception {
        final String committedOffset = new String(CURATOR.getData().forPath(offsetPath(partition)), UTF_8);
        assertThat(committedOffset, equalTo(offset));
    }

    private String offsetPath(final String partition) {
        return format("{0}/{1}/offsets/{2}/{3}", SUBSCRIPTIONS_PATH, sid, etName, partition);
    }

    private String sessionPath(final String sessionId) {
        return format("{0}/{1}/sessions/{2}", SUBSCRIPTIONS_PATH, sid, sessionId);
    }

    private String subscriptionPath() {
        return format("{0}/{1}", SUBSCRIPTIONS_PATH, sid);
    }

    private void setPartitions(final Partition[] partitions) throws Exception {
        final String topologyPath = subscriptionPath() + "/topology";
        final byte[] topologyData = MAPPER.writeValueAsBytes(new NewZkSubscriptionClient.Topology(partitions, 0));
        if (null == CURATOR.checkExists().forPath(topologyPath)) {
            CURATOR.create().forPath(topologyPath, topologyData);
        } else {
            CURATOR.setData().forPath(topologyPath, topologyData);
        }
    }

}
