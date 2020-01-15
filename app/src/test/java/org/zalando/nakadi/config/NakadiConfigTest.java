package org.zalando.nakadi.config;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetDataBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.core.env.Environment;
import org.zalando.nakadi.domain.storage.DefaultStorage;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.repository.db.StorageDbRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.service.StorageService;

import java.util.Optional;

public class NakadiConfigTest {

    private StorageDbRepository storageDbRepository = Mockito.mock(StorageDbRepository.class);
    private Environment environment = Mockito.mock(Environment.class);
    private ZooKeeperHolder zooKeeperHolder = Mockito.mock(ZooKeeperHolder.class);
    private GetDataBuilder dataBuilder = Mockito.mock(GetDataBuilder.class);

    @Before
    public void setUp() {
        final CuratorFramework curatorFramework = Mockito.mock(CuratorFramework.class);
        zooKeeperHolder = Mockito.mock(ZooKeeperHolder.class);
        Mockito.when(zooKeeperHolder.get()).thenReturn(curatorFramework);
        Mockito.when(curatorFramework.getData()).thenReturn(dataBuilder);
        Mockito.when(environment.getProperty("nakadi.zookeeper.connectionString"))
                .thenReturn("exhibitor://localhost:8181/path");
    }

    @Test
    public void shouldCreateDefaultStorageWhenNoDefaultStorageInZk() throws Exception {
        Mockito.when(dataBuilder.forPath(StorageService.ZK_TIMELINES_DEFAULT_STORAGE)).thenReturn(null);
        Mockito.when(environment.getProperty("nakadi.timelines.storage.default")).thenReturn("default");
        Mockito.when(storageDbRepository.getStorage("default")).thenReturn(Optional.ofNullable(null));
        final DefaultStorage defaultStorage =
                new NakadiConfig().defaultStorage(storageDbRepository, environment, zooKeeperHolder);
        Assert.assertEquals("default", defaultStorage.getStorage().getId());
    }

    @Test
    public void shouldTakeStorageById() throws Exception {
        Mockito.when(dataBuilder.forPath(StorageService.ZK_TIMELINES_DEFAULT_STORAGE))
                .thenReturn("new-storage".getBytes());
        final Storage storage = new Storage();
        storage.setId("new-storage");
        Mockito.when(storageDbRepository.getStorage("new-storage")).thenReturn(Optional.of(storage));
        final DefaultStorage defaultStorage =
                new NakadiConfig().defaultStorage(storageDbRepository, environment, zooKeeperHolder);
        Assert.assertEquals("new-storage", defaultStorage.getStorage().getId());
    }

}
