package org.zalando.nakadi.repository;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;

public class TopicRepositoryHolderTest {

    @Test
    public void testGetTopicRepository() {
        final TopicRepositoryHolder topicRepositoryHolder = new TopicRepositoryHolder(new TestTopicRepository());
        final Storage storage = new Storage();
        storage.setType(Storage.Type.KAFKA);

        Assert.assertNotNull(topicRepositoryHolder.getTopicRepository(storage));
    }

    @Test(expected = TopicRepositoryException.class)
    public void testGetTopicRepositoryNoSuchType() {
        final TopicRepositoryHolder topicRepositoryHolder = new TopicRepositoryHolder(new TestTopicRepository());
        final Storage storage = new Storage();
        topicRepositoryHolder.getTopicRepository(storage);
    }

    @Test
    public void testTopicRepositoryCreatedOnce() {
        final TopicRepositoryHolder holder = new TopicRepositoryHolder(new TestTopicRepository());
        Assert.assertSame(
                holder.getTopicRepository(new Storage("1", Storage.Type.KAFKA)),
                holder.getTopicRepository(new Storage("1", Storage.Type.KAFKA)));
    }

    @Test(timeout = 5000L)
    public void testLockingWhileRepoCreation() throws InterruptedException {
        final Storage storage = new Storage("1", Storage.Type.KAFKA);
        final TopicRepository[] repos = new TopicRepository[10];
        final AtomicInteger callCount = new AtomicInteger(0);
        final CountDownLatch allowCreation = new CountDownLatch(1);
        final CountDownLatch allRequested = new CountDownLatch(repos.length);
        final CountDownLatch allCreated = new CountDownLatch(repos.length);
        final TopicRepositoryCreator creator = new TopicRepositoryCreator() {

            @Override
            public TopicRepository createTopicRepository(final Storage storage) throws TopicRepositoryException {
                try {
                    allowCreation.await();
                } catch (InterruptedException ignore) {
                }
                callCount.incrementAndGet();
                return mock(TopicRepository.class);
            }

            @Override
            public Timeline.StoragePosition createStoragePosition(final List<NakadiCursor> offsets)
                    throws NakadiRuntimeException {
                return null;
            }

            @Override
            public Storage.Type getSupportedStorageType() {
                return Storage.Type.KAFKA;
            }
        };
        final TopicRepositoryHolder holder = new TopicRepositoryHolder(creator);
        for (int i = 0; i < repos.length; ++i) {
            final int currentIdx = i;
            new Thread(() -> {
                allRequested.countDown();
                repos[currentIdx] = holder.getTopicRepository(storage);
                allCreated.countDown();
            }).start();
        }
        allRequested.await();
        Thread.sleep(100L);
        Stream.of(repos).forEach(Assert::assertNull);
        allowCreation.countDown();
        allCreated.await();
        Stream.of(repos).forEach(Assert::assertNotNull);
        Assert.assertEquals(1, Stream.of(repos).collect(Collectors.toSet()).size());
        Assert.assertEquals(1, callCount.get());
    }

    private class TestTopicRepository implements TopicRepositoryCreator {

        @Override
        public TopicRepository createTopicRepository(final Storage storage) throws TopicRepositoryException {
            return mock(TopicRepository.class);
        }

        @Override
        public Timeline.StoragePosition createStoragePosition(final List<NakadiCursor> offsets)
                throws NakadiRuntimeException {
            return null;
        }

        @Override
        public Storage.Type getSupportedStorageType() {
            return Storage.Type.KAFKA;
        }
    }
}