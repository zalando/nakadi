package org.zalando.nakadi.repository;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;

import java.util.List;

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

    private class TestTopicRepository implements TopicRepositoryCreator {

        @Override
        public TopicRepository createTopicRepository(final Storage storage) throws TopicRepositoryException {
            return Mockito.mock(TopicRepository.class);
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