package org.zalando.nakadi.repository.db;

import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.repository.EventTypeRepository;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class CachingEventTypeRepositoryTest {

    private final EventTypeRepository dbRepo = mock(EventTypeRepository.class);
    private final EventTypeCache cache = mock(EventTypeCache.class);
    private final EventTypeRepository cachedRepo;
    private final EventType et = mock(EventType.class);

    public CachingEventTypeRepositoryTest() throws Exception {
        this.cachedRepo = new CachingEventTypeRepository(dbRepo, cache);

        Mockito
                .doReturn("event-name")
                .when(et)
                .getName();
    }

    @Test
    public void whenDbPersistenceSucceedThenNotifyCache() throws Exception {
        cachedRepo.saveEventType(et);

        verify(dbRepo, times(1)).saveEventType(et);
        verify(cache, times(1)).created(et.getName());
    }

    @Test
    public void whenCacheFailsThenRollbackEventPersistence() throws Exception {
        Mockito
                .doThrow(Exception.class)
                .when(cache)
                .created("event-name");

        try {
            cachedRepo.saveEventType(et);
        } catch (InternalNakadiException e) {
            verify(dbRepo, times(1)).removeEventType("event-name");
        }
    }

    @Test
    public void whenDbUpdateSucceedThenNotifyCache() throws Exception {
        cachedRepo.update(et);

        verify(dbRepo, times(1)).update(et);
        verify(cache, times(1)).updated("event-name");
    }

    @Test
    public void whenUpdateCacheFailThenRollbackDbPersistence() throws Exception {
        Mockito
                .doThrow(Exception.class)
                .when(cache)
                .updated("event-name");

        final EventType original = mock(EventType.class);

        Mockito
                .doReturn(original)
                .when(dbRepo)
                .findByName("event-name");

        try {
            cachedRepo.update(et);
        } catch (InternalNakadiException e) {
            verify(dbRepo, times(1)).update(original);
        }
    }

    @Test
    public void removeFromDbSucceedNotifyCache() throws Exception {
        cachedRepo.removeEventType("event-name");

        verify(dbRepo, times(1)).removeEventType("event-name");
        verify(cache, times(1)).removed("event-name");
    }

    @Test
    public void whenRemoveCacheEntryFailsThenRollbackDbRemoval() throws Exception {
        Mockito
                .doThrow(Exception.class)
                .when(cache)
                .removed("event-name");

        final EventType original = mock(EventType.class);

        Mockito
                .doReturn(original)
                .when(dbRepo)
                .findByName("event-name");

        try {
            cachedRepo.removeEventType("event-name");
        } catch (InternalNakadiException e) {
            verify(dbRepo, times(1)).saveEventType(original);
        }
    }
}
