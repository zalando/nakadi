package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiWrapperException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;

import java.util.List;

public interface TopicRepositoryCreator {

    TopicRepository createTopicRepository(Storage storage) throws TopicRepositoryException;

    Timeline.StoragePosition createStoragePosition(List<NakadiCursor> offsets) throws NakadiWrapperException;

    Storage.Type getSupportedStorageType();
}
