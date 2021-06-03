package org.zalando.nakadi.repository.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class CuratorFrameworkRotatorTest {

    @Test
    public void testShouldReplaceCurator() throws InterruptedException {
        final ZooKeeperHolder zkh = Mockito.mock(ZooKeeperHolder.class);
        final CuratorFramework cf1 = Mockito.mock(CuratorFramework.class);
        final CuratorFramework cf2 = Mockito.mock(CuratorFramework.class);

        Mockito.when(zkh.newCuratorFramework()).thenReturn(cf1);

        final CuratorFrameworkRotator curatorFrameworkRotator = new CuratorFrameworkRotator(zkh, 10, 10000);

        CuratorFramework cfTmp = curatorFrameworkRotator.takeCuratorFramework();
        Assert.assertEquals(cf1, cfTmp);
        curatorFrameworkRotator.returnCuratorFramework(cfTmp);

        Thread.sleep(15);
        Mockito.when(zkh.newCuratorFramework()).thenReturn(cf2);
        curatorFrameworkRotator.scheduleRotationCheck();

        cfTmp = curatorFrameworkRotator.takeCuratorFramework();
        Assert.assertEquals(cf2, cfTmp);
        curatorFrameworkRotator.returnCuratorFramework(cfTmp);
    }

    @Test
    public void testShouldReturnNewCuratorIfRetriedCuratorIsClosed() throws InterruptedException {
        final ZooKeeperHolder zkh = Mockito.mock(ZooKeeperHolder.class);
        final CuratorFramework cf1 = Mockito.mock(CuratorFramework.class, "cf1");
        final CuratorFramework cf2 = Mockito.mock(CuratorFramework.class, "cf2");
        final CuratorFramework cf3 = Mockito.mock(CuratorFramework.class, "cf3");

        Mockito.when(zkh.newCuratorFramework()).thenReturn(cf1);

        final CuratorFrameworkRotator curatorFrameworkRotator = new CuratorFrameworkRotator(zkh, 10, 10000);

        CuratorFramework cfTmp = curatorFrameworkRotator.takeCuratorFramework();
        Assert.assertEquals(cf1, cfTmp);
        // do not return curator

        Thread.sleep(20);
        Mockito.when(zkh.newCuratorFramework()).thenReturn(cf2);
        curatorFrameworkRotator.scheduleRotationCheck();
        cfTmp = curatorFrameworkRotator.takeCuratorFramework();
        Assert.assertEquals(cf2, cfTmp);

        Thread.sleep(20);
        Mockito.when(zkh.newCuratorFramework()).thenReturn(cf3);
        curatorFrameworkRotator.scheduleRotationCheck();
        cfTmp = curatorFrameworkRotator.takeCuratorFramework();
        Assert.assertEquals(cf2, cfTmp);

        // finally return client and expect new client will be created
        curatorFrameworkRotator.returnCuratorFramework(cf1);

        Thread.sleep(20);
        Mockito.when(zkh.newCuratorFramework()).thenReturn(cf3);
        // it will nullify retired client
        curatorFrameworkRotator.scheduleRotationCheck();
        //need second call to rotate the client
        curatorFrameworkRotator.scheduleRotationCheck();
        cfTmp = curatorFrameworkRotator.takeCuratorFramework();
        Assert.assertEquals(cf3, cfTmp);
    }

}
