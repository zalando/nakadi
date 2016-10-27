package org.zalando.nakadi.util;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@Component
public class ZkConfigurationService {

    private static final Logger LOG = LoggerFactory.getLogger(ZkConfigurationService.class);
    private static final String PREFIX = "/nakadi/configuration/";

    private static final Charset CHARSET = StandardCharsets.UTF_8;

    private final ZooKeeperHolder zooKeeperHolder;
    private final LoadingCache<String, String> cache;
    private final ApplicationContext context;

    @Autowired
    public ZkConfigurationService(final ZooKeeperHolder zooKeeperHolder, final ApplicationContext context) {
        this.zooKeeperHolder = zooKeeperHolder;
        this.context = context;
        this.cache = CacheBuilder.newBuilder()
                .expireAfterWrite(1, TimeUnit.MINUTES)
                .build(new CacheLoader<String, String>() {
                    public String load(final String key) throws Exception {
                        return getValue(key);
                    }
                });
    }

    public Long getLong(final String key, final Long fallback) {
        try {
            return Long.parseLong(cache.getUnchecked(key));
        } catch (NumberFormatException e) {
            LOG.error("Can't obtain value from ZK", e);
            return fallback;
        }
    }

    private String getValue(final String key) {
        try {
            final byte[] data = zooKeeperHolder.get().getData().forPath(PREFIX + key);
            return new String(data, CHARSET);
        } catch (Exception e) {
            LOG.error("Can't obtain value from ZK", e);
            return context.getEnvironment().getProperty(key);
        }
    }
}
