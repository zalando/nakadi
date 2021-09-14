package org.zalando.nakadi.service.job;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

/**
 * Class helps to run jobs that should be performed periodically only on single Nakadi instance
 */
public class ExclusiveJobWrapper {

    public static final String NAKADI_JOBS_PATH = "/nakadi/jobs";

    private final Logger log;
    private final String zkPath;
    private final ZooKeeperHolder zkHolder;
    private final ObjectMapper objectMapper;
    private final long jobPeriodMs;

    public ExclusiveJobWrapper(final ZooKeeperHolder zkHolder,
                               final ObjectMapper objectMapper,
                               final String jobName,
                               final long jobPeriodMs) {
        this.zkHolder = zkHolder;
        this.objectMapper = objectMapper;
        this.jobPeriodMs = jobPeriodMs;
        this.zkPath = ZKPaths.makePath(NAKADI_JOBS_PATH, jobName);
        this.log = LoggerFactory.getLogger("nakadi-job." + jobName);
    }

    public void runJobLocked(final Runnable action) {
        log.info("Trying to run job...");

        if (!timeToRunAction()) {
            return;
        }
        if (!createJobLockNode()) {
            return;
        }
        try {
            // we need to check it again after we acquired the lock
            if (!timeToRunAction()) {
                return;
            }
            log.info("Job will be executed on this instance");
            action.run();
            updateJobTimestamp();
        } catch (final JsonProcessingException e) {
            log.error("Json processing error when updating job timestamp in ZK", e);
        } catch (final Exception e) {
            log.error("Zookeeper error when performing job", e);
        } finally {
            deleteJobLockNode();
        }
    }

    private void deleteJobLockNode() {
        // delete the node that is used as a lock to be
        // captured by Nakadi instance that performs job
        try {
            zkHolder.get()
                    .delete()
                    .forPath(zkPath + "/lock");
        } catch (final Exception e) {
            log.error("Zookeeper error when deleting job lock-node", e);
        }
    }

    private void updateJobTimestamp() throws Exception {
        try {
            // try to create node for the case if it doesn't exist
            zkHolder.get()
                    .create()
                    .creatingParentsIfNeeded()
                    .forPath(zkPath + "/latest");
        } catch (final KeeperException.NodeExistsException e) {
            // "latest" node may already exist - this is ok
        }
        // update the latest timestamp of job
        final DateTime now = new DateTime(DateTimeZone.UTC);
        final byte[] currentTimeAsBytes = objectMapper.writeValueAsString(now).getBytes(Charsets.UTF_8);
        zkHolder.get()
                .setData()
                .forPath(zkPath + "/latest", currentTimeAsBytes);
    }

    private boolean createJobLockNode() {
        try {
            zkHolder.get()
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(zkPath + "/lock");
        } catch (final KeeperException.NodeExistsException e) {
            // another node already performs this job
            log.info("Job will not be run on this node as it is performed on other node");
            return false;
        } catch (final Exception e) {
            log.error("Zookeeper error when creating job lock-node", e);
            return false;
        }
        return true;
    }

    private boolean timeToRunAction() {
        try {
            final byte[] data = zkHolder.get()
                    .getData()
                    .forPath(zkPath + "/latest");
            final DateTime lastCleaned = objectMapper.readValue(new String(data, Charsets.UTF_8), DateTime.class);
            final DateTime now = new DateTime(DateTimeZone.UTC);
            return now.getMillis() - lastCleaned.getMillis() > jobPeriodMs;
        } catch (final KeeperException.NoNodeException e) {
            // if the node doesn't exist - that means that the job
            // was never performed and we need to perform it
            return true;
        } catch (final Exception e) {
            log.error("Zookeeper error when checking last clean time", e);
            return false;
        }
    }

}
