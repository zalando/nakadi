package org.zalando.nakadi.service.job;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

public class JobWrapperFactory {

    private final ZooKeeperHolder zkHolder;
    private final ObjectMapper objectMapper;

    public JobWrapperFactory(final ZooKeeperHolder zkHolder, final ObjectMapper objectMapper) {
        this.zkHolder = zkHolder;
        this.objectMapper = objectMapper;
    }

    public ExclusiveJobWrapper createExclusiveJobWrapper(final String jobName, final long jobPeriodMs) {
        return new ExclusiveJobWrapper(zkHolder, objectMapper, jobName, jobPeriodMs);
    }
}
