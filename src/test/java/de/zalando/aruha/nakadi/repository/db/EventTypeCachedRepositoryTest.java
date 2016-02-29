package de.zalando.aruha.nakadi.repository.db;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import org.junit.Test;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class EventTypeCachedRepositoryTest {

    private final EventTypeRepository dbRepo = mock(EventTypeRepository.class);
    private final EventTypeCache cache = mock(EventTypeCache.class);
    private final EventTypeRepository cachedRepo;
    private final EventType et = mock(EventType.class);

    public EventTypeCachedRepositoryTest() throws Exception {
        this.cachedRepo = new EventTypeCachedRepository(dbRepo, cache);

        Mockito
                .doReturn("event-name")
                .when(et)
                .getName();
    }

    @Test
    public void whenDbPersistenceSucceedThenNotifyCache() throws Exception {
        cachedRepo.saveEventType(et);

        verify(dbRepo, times(1)).saveEventType(et);
        verify(cache, times(1)).created(et);
    }

    @Test
    public void whenCacheFailsThenRollbackEventPersistence() throws Exception {
        Mockito
                .doThrow(Exception.class)
                .when(cache)
                .created(et);

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

        EventType original = mock(EventType.class);

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

        EventType original = mock(EventType.class);

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
