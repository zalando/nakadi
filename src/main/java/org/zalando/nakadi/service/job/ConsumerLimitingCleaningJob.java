package org.zalando.nakadi.service.job;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.ConsumerLimitingService;

import static org.zalando.nakadi.service.ConsumerLimitingService.CONNECTIONS_ZK_PATH;

@Service
public class ConsumerLimitingCleaningJob {

    public static final String JOB_NAME = "consumer-nodes-cleanup";

    private static final Logger LOG = LoggerFactory.getLogger(ConsumerLimitingCleaningJob.class);

    private final ZooKeeperHolder zkHolder;
    private final ConsumerLimitingService limitingService;
    private final ExclusiveJobWrapper jobWrapper;

    @Autowired
    public ConsumerLimitingCleaningJob(final ZooKeeperHolder zkHolder,
                                       final JobWrapperFactory jobWrapperFactory,
                                       final ConsumerLimitingService limitingService,
                                       @Value("${nakadi.jobs.consumerNodesCleanup.runPeriodMs}") final int periodMs) {
        this.zkHolder = zkHolder;
        this.limitingService = limitingService;
        jobWrapper = jobWrapperFactory.createExclusiveJobWrapper(JOB_NAME, periodMs);
    }

    @Scheduled(
            fixedDelayString = "${nakadi.jobs.checkRunMs}",
            initialDelayString = "${random.int(${nakadi.jobs.checkRunMs})}")
    public void cleanHangingNodes() {
        jobWrapper.runJobLocked(this::cleanHangingConsumersNodes);
    }

    private void cleanHangingConsumersNodes() {
        // try to remove every consumer node;
        // the nodes that have children will fail to be removed
        try {
            zkHolder.get()
                    .getChildren()
                    .forPath(CONNECTIONS_ZK_PATH)
                    .forEach(limitingService::deletePartitionNodeIfPossible);
        } catch (final KeeperException.NoNodeException e) {
            LOG.debug("ZK node for connections doesn't exist");
        } catch (final Exception e) {
            LOG.error("ZK error when cleaning consumer nodes");
        }
    }

}
