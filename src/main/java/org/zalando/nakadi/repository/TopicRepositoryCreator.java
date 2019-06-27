package org.zalando.nakadi.repository;

import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;

import java.util.List;

public interface TopicRepositoryCreator {

    TopicRepository createTopicRepository(Storage storage) throws TopicRepositoryException;

    Timeline.StoragePosition createStoragePosition(List<NakadiCursor> offsets) throws NakadiRuntimeException;

    Storage.Type getSupportedStorageType();
}
