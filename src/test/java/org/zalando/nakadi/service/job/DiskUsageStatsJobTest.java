package org.zalando.nakadi.service.job;

import org.junit.Test;
import org.zalando.nakadi.domain.Storage;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.TopicRepositoryHolder;
import org.zalando.nakadi.repository.db.TimelineDbRepository;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DiskUsageStatsJobTest {

    @Test
    public void testLoadDiskUsage() {

        final TimelineDbRepository timelineDbRepository = mock(TimelineDbRepository.class);
        final TopicRepositoryHolder topicRepositoryHolder = mock(TopicRepositoryHolder.class);
        final DiskUsageStatsJob job = new DiskUsageStatsJob(
                mock(JobWrapperFactory.class),
                timelineDbRepository,
                topicRepositoryHolder,
                null,
                null,
                new DiskUsageStatsConfig());

        // Et  | Top      | Partitions | presence in several storages
        // et1 | t1       | 1          | false
        // et2 | t2       | 2          | false
        // et3 | t31, t32 | 1          | true
        // et4 | t41, t42 | 2          | true

        final Storage storage1 = new Storage("id1", Storage.Type.KAFKA);
        final Storage storage2 = new Storage("id2", Storage.Type.KAFKA);

        final Timeline t1 = new Timeline("et1", 0, storage1, "t1", new Date());
        final Timeline t2 = new Timeline("et2", 0, storage1, "t2", new Date());
        final Timeline t31 = new Timeline("et3", 0, storage1, "t31", new Date());
        final Timeline t32 = new Timeline("et3", 0, storage2, "t32", new Date());
        final Timeline t41 = new Timeline("et4", 0, storage1, "t41", new Date());
        final Timeline t42 = new Timeline("et4", 0, storage2, "t42", new Date());

        when(timelineDbRepository.listTimelinesOrdered()).thenReturn(Arrays.asList(t1, t2, t31, t32, t41, t42));

        final TopicRepository storage1TopicRepo = mock(TopicRepository.class);
        when(topicRepositoryHolder.getTopicRepository(eq(storage1))).thenReturn(storage1TopicRepo);
        final Map<TopicPartition, Long> storage1SizeStats = new HashMap<>();
        storage1SizeStats.put(new TopicPartition("t1", "0"), 5L);
        storage1SizeStats.put(new TopicPartition("t2", "0"), 7L);
        storage1SizeStats.put(new TopicPartition("t2", "1"), 11L);
        storage1SizeStats.put(new TopicPartition("t31", "0"), 13L);
        storage1SizeStats.put(new TopicPartition("t41", "0"), 17L);
        storage1SizeStats.put(new TopicPartition("t41", "1"), 19L);
        when(storage1TopicRepo.getSizeStats()).thenReturn(storage1SizeStats);

        final TopicRepository storage2TopicRepo = mock(TopicRepository.class);
        when(topicRepositoryHolder.getTopicRepository(eq(storage2))).thenReturn(storage2TopicRepo);
        final Map<TopicPartition, Long> storage2SizeStats = new HashMap<>();
        storage2SizeStats.put(new TopicPartition("t32", "0"), 500L);
        storage2SizeStats.put(new TopicPartition("t42", "0"), 700L);
        storage2SizeStats.put(new TopicPartition("t42", "1"), 1100L);
        when(storage2TopicRepo.getSizeStats()).thenReturn(storage2SizeStats);

        final Map<String, Long> expectedResult = new HashMap<>();
        expectedResult.put("et1", 5L);
        expectedResult.put("et2", 18L);
        expectedResult.put("et3", 513L);
        expectedResult.put("et4", 1836L);

        final Map<String, Long> actualResult = job.loadDiskUsage();
        assertEquals(expectedResult, actualResult);
    }
}