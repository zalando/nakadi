package de.zalando.aruha.nakadi.service;

import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperLockFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.curator.framework.api.SetDataBuilder;
import org.apache.curator.framework.recipes.locks.InterProcessLock;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.Charset;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CursorsCommitServiceTest {

    public static final Charset CHARSET = Charset.forName("UTF-8");

    private TopicRepository topicRepository;
    private CursorsCommitService cursorsCommitService;
    private GetDataBuilder getDataBuilder;
    private SetDataBuilder setDataBuilder;

    @Before
    public void before() {
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

        cursorsCommitService = new CursorsCommitService(zkHolder, topicRepository, zkLockFactory);
    }

    @Test
    public void whenCommitCursorsThenTrue() throws Exception {
        when(getDataBuilder.forPath(any())).thenReturn("oldOffset".getBytes(CHARSET));
        when(topicRepository.compareOffsets("newOffset", "oldOffset")).thenReturn(1);

        final boolean committed = cursorsCommitService.commitCursor("sid", "my-et", new Cursor("p1", "newOffset"));

        assertThat(committed, is(true));
        verify(setDataBuilder, times(1))
                .forPath(eq("/nakadi/subscriptions/sid/topics/my-et/p1/offset"), eq("newOffset".getBytes(CHARSET)));
    }

    @Test
    public void whenCommitOldCursorsThenFalse() throws Exception {
        when(getDataBuilder.forPath(any())).thenReturn("oldOffset".getBytes(CHARSET));
        when(topicRepository.compareOffsets("newOffset", "oldOffset")).thenReturn(-1);

        final boolean committed = cursorsCommitService.commitCursor("sid", "my-et", new Cursor("p1", "newOffset"));

        assertThat(committed, is(false));
        verify(setDataBuilder, never()).forPath(any(), any());
    }

    @Test(expected = ServiceUnavailableException.class)
    public void whenExceptionThenServiceUnavailableException() throws Exception {
        when(getDataBuilder.forPath(any())).thenThrow(new Exception());
        cursorsCommitService.commitCursor("sid", "my-et", new Cursor("p1", "newOffset"));
    }
}
