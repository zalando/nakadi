package org.zalando.nakadi.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.HIGH_LEVEL_API;

public class FeatureToggleServiceTest {

    public static final String TEST_FEATURE_ZK_PATH = "/nakadi/feature_toggle/" + HIGH_LEVEL_API.getId();

    private FeatureToggleService featureToggleService;
    private ExistsBuilder existsBuilder;

    @Before
    public void before() {
        existsBuilder = mock(ExistsBuilder.class);

        final CuratorFramework curatorFramework = mock(CuratorFramework.class);
        when(curatorFramework.checkExists()).thenReturn(existsBuilder);

        final ZooKeeperHolder zkHolder = mock(ZooKeeperHolder.class);
        when(zkHolder.get()).thenReturn(curatorFramework);

        featureToggleService = new FeatureToggleServiceZk(zkHolder);
    }

    @Test
    public void whenFeatureEnabledThenTrue() throws Exception {
        when(existsBuilder.forPath(TEST_FEATURE_ZK_PATH)).thenReturn(new Stat());
        final boolean featureEnabled = featureToggleService.isFeatureEnabled(HIGH_LEVEL_API);
        assertThat(featureEnabled, is(true));
    }

    @Test
    public void whenFeatureDisabledThenFalse() throws Exception {
        when(existsBuilder.forPath(TEST_FEATURE_ZK_PATH)).thenReturn(null);
        final boolean featureEnabled = featureToggleService.isFeatureEnabled(HIGH_LEVEL_API);
        assertThat(featureEnabled, is(false));
    }

    @Test
    public void whenExceptionThenFalse() throws Exception {
        when(existsBuilder.forPath(TEST_FEATURE_ZK_PATH)).thenThrow(new Exception());
        final boolean featureEnabled = featureToggleService.isFeatureEnabled(HIGH_LEVEL_API);
        assertThat(featureEnabled, is(false));
    }

}
