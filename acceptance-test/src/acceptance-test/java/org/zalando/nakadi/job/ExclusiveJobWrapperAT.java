package org.zalando.nakadi.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.hamcrest.CoreMatchers;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.job.ExclusiveJobWrapper;
import org.zalando.nakadi.webservice.BaseAT;
import org.zalando.nakadi.webservice.utils.ZookeeperTestUtils;

import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;
import static org.zalando.nakadi.utils.TestUtils.randomUUID;

public class ExclusiveJobWrapperAT extends BaseAT {

    private static final CuratorFramework CURATOR = ZookeeperTestUtils.createCurator(ZOOKEEPER_URL);
    private static final int TEST_JOB_PERIOD_MS = 3 * 60 * 60 * 1000; // 3 hours

    private ObjectMapper objectMapper;
    private final ZooKeeperHolder zkHolder;
    private AtomicBoolean jobExecuted;
    private Runnable dummyJob;
    private ExclusiveJobWrapper jobWrapper;
    private String lockPath;
    private String latestPath;

    public ExclusiveJobWrapperAT() {
        objectMapper = new JsonConfig().jacksonObjectMapper();
        zkHolder = Mockito.mock(ZooKeeperHolder.class);
        Mockito.when(zkHolder.get()).thenReturn(CURATOR);
    }

    @Before
    public void before() throws Exception {
        final String testJobName = randomUUID();

        lockPath = ZKPaths.makePath(ExclusiveJobWrapper.NAKADI_JOBS_PATH, testJobName, "lock");
        latestPath = ZKPaths.makePath(ExclusiveJobWrapper.NAKADI_JOBS_PATH, testJobName, "latest");

        jobExecuted = new AtomicBoolean(false);
        dummyJob = () -> jobExecuted.set(true);

        jobWrapper = new ExclusiveJobWrapper(zkHolder, objectMapper, testJobName, TEST_JOB_PERIOD_MS);
    }

    @Test
    public void whenItsNotTimeToRunThenJobIsNotExecuted() throws Exception {
        // set latest job execution as two hours ago
        createLatestNode(2);

        jobWrapper.runJobLocked(dummyJob);
        assertThat(jobExecuted.get(), CoreMatchers.is(false));
    }

    @Test
    public void whenJobIsAlreadyRunningOnAnotherNodeThenJobIsNotExecuted() throws Exception {
        // set latest job execution as 30 hours ago
        createLatestNode(30);
        // another Nakadi instance already performs job
        CURATOR.create().creatingParentsIfNeeded().forPath(lockPath);

        jobWrapper.runJobLocked(dummyJob);
        assertThat(jobExecuted.get(), CoreMatchers.is(false));
    }

    @Test
    public void whenExecuteJobThenOk() throws Exception {
        jobWrapper.runJobLocked(dummyJob);
        assertThat(jobExecuted.get(), CoreMatchers.is(true));

        assertThat("lock node should be removed", CURATOR.checkExists().forPath(lockPath),
                CoreMatchers.is(CoreMatchers.nullValue()));

        final byte[] data = CURATOR.getData().forPath(latestPath);
        final DateTime lastCleaned = objectMapper.readValue(new String(data, Charsets.UTF_8), DateTime.class);
        final long msSinceCleaned = new DateTime().getMillis() - lastCleaned.getMillis();
        assertThat("job was executed less than 5 seconds ago", msSinceCleaned, lessThan(5000L));

    }

    private void createLatestNode(final int hoursAgo) throws Exception {
        final DateTime now = new DateTime(DateTimeZone.UTC);
        final DateTime pastDate = now.minusHours(hoursAgo);
        final byte[] data = objectMapper.writeValueAsString(pastDate).getBytes(Charsets.UTF_8);
        CURATOR.create().creatingParentsIfNeeded().forPath(latestPath, data);
    }
}
