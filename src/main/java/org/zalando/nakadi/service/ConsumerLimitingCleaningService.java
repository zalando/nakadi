package org.zalando.nakadi.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Hours;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.nio.charset.Charset;

import static org.zalando.nakadi.service.ConsumerLimitingService.CONNECTIONS_ZK_PATH;

@Service
public class ConsumerLimitingCleaningService {

    public static final String CLEANING_ZK_PATH = "/nakadi/consumers/cleaning";

    private static final Charset CHARSET_UTF8 = Charset.forName("UTF-8");
    private static final Logger LOG = LoggerFactory.getLogger(CursorsService.class);
    private static final int HANGING_NODES_CLEAN_PERIOD_H = 6;

    private final ZooKeeperHolder zkHolder;
    private final ObjectMapper objectMapper;
    private final ConsumerLimitingService limitingService;

    @Autowired
    public ConsumerLimitingCleaningService(final ZooKeeperHolder zkHolder, final ObjectMapper objectMapper,
                                           final ConsumerLimitingService limitingService) {
        this.zkHolder = zkHolder;
        this.objectMapper = objectMapper;
        this.limitingService = limitingService;
    }

    @Scheduled(fixedRate = HANGING_NODES_CLEAN_PERIOD_H * 3600000)
    public void cleanHangingNodes() {
        LOG.info("Trying to run cleaning of 'hanging' connection ZK nodes");

        if (!timeToRunCleaning()) {
            return;
        }
        if (!createCleaningLockNode()) {
            return;
        }
        try {
            LOG.info("Cleaning of 'hanging' connection ZK nodes will be performed on this instance");
            cleanHangingConsumersNodes();
            updateCleaningTimestamp();
        } catch (JsonProcessingException e) {
            LOG.error("Json processing error when updating cleaning timestamp in ZK", e);
        } catch (Exception e) {
            LOG.error("Zookeeper error when cleaning consumer nodes in ZK", e);
        } finally {
            deleteCleaningLockNode();
        }
    }

    private void cleanHangingConsumersNodes() throws Exception {
        // try to remove every consumer node;
        // the nodes that have children will fail to be removed
        zkHolder.get()
                .getChildren()
                .forPath(CONNECTIONS_ZK_PATH)
                .forEach(child -> limitingService.deletePartitionNodeIfPossible(CONNECTIONS_ZK_PATH + "/" + child));
    }

    private void deleteCleaningLockNode() {
        // delete the node that is used as a lock to be
        // captured by Nakadi instance that performs cleaning
        try {
            zkHolder.get()
                    .delete()
                    .guaranteed()
                    .forPath(CLEANING_ZK_PATH + "/lock");
        } catch (Exception e) {
            LOG.error("Zookeeper error when deleting consumer cleaning lock-node", e);
        }
    }

    private void updateCleaningTimestamp() throws Exception {
        try {
            // try to create node for the case if it doesn't exist
            zkHolder.get()
                    .create()
                    .creatingParentsIfNeeded()
                    .forPath(CLEANING_ZK_PATH + "/latest");
        } catch (final KeeperException.NodeExistsException e) {
            // "latest" node may already exist - this is ok
        }
        // update the latest timestamp of cleaning
        final DateTime now = new DateTime(DateTimeZone.UTC);
        final byte[] currentTimeAsBytes = objectMapper.writeValueAsString(now).getBytes(CHARSET_UTF8);
        zkHolder.get()
                .setData()
                .forPath(CLEANING_ZK_PATH + "/latest", currentTimeAsBytes);
    }

    private boolean createCleaningLockNode() {
        try {
            zkHolder.get()
                    .create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(CLEANING_ZK_PATH + "/lock");
        } catch (final KeeperException.NodeExistsException e) {
            // another node already performs cleaning
            LOG.info("Consumers deprecated nodes cleaning is cancelled as it is performed on other node", e);
            return false;
        } catch (Exception e) {
            LOG.error("Zookeeper error when creating consumer cleaning lock-node", e);
            return false;
        }
        return true;
    }

    private boolean timeToRunCleaning() {
        try {
            final byte[] data = zkHolder.get()
                    .getData()
                    .forPath(CLEANING_ZK_PATH + "/latest");
            final DateTime lastCleaned = objectMapper.readValue(new String(data, CHARSET_UTF8), DateTime.class);
            final DateTime now = new DateTime(DateTimeZone.UTC);
            return Hours.hoursBetween(lastCleaned, now).getHours() > HANGING_NODES_CLEAN_PERIOD_H;
        } catch (final KeeperException.NoNodeException e) {
            // if the node doesn't exist - that means that the clean
            // was never performed and we need to perform it
            return true;
        } catch (final Exception e) {
            LOG.error("Zookeeper error when checking last clean time", e);
            return false;
        }
    }

}
