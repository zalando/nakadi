package org.zalando.nakadi.repository.kafka;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.GetChildrenBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;

import java.util.List;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class KafkaLocationManagerTest {
    @Test
    public void testItAppliesSecuritySettings() {
        final var zkFactoryMock = mock(ZooKeeperHolder.class);
        final var kafkaSettingsMock = mock(KafkaSettings.class);

        when(kafkaSettingsMock.getSecurityProtocol()).thenReturn(Optional.of("SASL_PLAINTEXT"));
        when(kafkaSettingsMock.getSaslMechanism()).thenReturn(Optional.of("PLAIN"));
        when(kafkaSettingsMock.getKafkaUsername()).thenReturn(Optional.of("nakadi_user"));
        when(kafkaSettingsMock.getKafkaPassword()).thenReturn(Optional.of("nakadi_password"));

        final var kafkaLocationManager = new KafkaLocationManager(zkFactoryMock, kafkaSettingsMock);
        final var kafkaProperties = kafkaLocationManager.getProperties();

        Assertions.assertEquals("SASL_PLAINTEXT",
                kafkaProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        Assertions.assertEquals("PLAIN", kafkaProperties.get(SaslConfigs.SASL_MECHANISM));
        Assertions.assertEquals("org.apache.kafka.common.security.plain.PlainLoginModule required " +
                        "username=nakadi_user password=\"nakadi_password\";",
                kafkaProperties.get(SaslConfigs.SASL_JAAS_CONFIG));

    }

    @Test
    public void testItAppliesSecurityOnlyIfAllRequiredConfigsAreProvided() {
        final var zkFactoryMock = mock(ZooKeeperHolder.class);
        final var kafkaSettingsMock = mock(KafkaSettings.class);

        when(kafkaSettingsMock.getSecurityProtocol()).thenReturn(Optional.of("SASL_PLAINTEXT"));

        final var kafkaLocationManager = new KafkaLocationManager(zkFactoryMock, kafkaSettingsMock);
        final var kafkaProperties = kafkaLocationManager.getProperties();

        Assertions.assertEquals(null, kafkaProperties.get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
        Assertions.assertEquals(null, kafkaProperties.get(SaslConfigs.SASL_MECHANISM));
        Assertions.assertEquals(null, kafkaProperties.get(SaslConfigs.SASL_JAAS_CONFIG));
    }

    @Test
    public void testItGetsTheListOfBrokersFromZookeeperButListenerPortFromConfig() throws Exception {
        final var zkFactoryMock = mock(ZooKeeperHolder.class);
        final var kafkaSettingsMock = mock(KafkaSettings.class);
        final var curatorMock = mock(CuratorFramework.class);
        final var childrenBuilderMock = mock(GetChildrenBuilder.class);
        final var getDataBuilderMock = mock(GetDataBuilder.class);

        //Configuration mocks
        when(kafkaSettingsMock.getPreferredListenerPort()).thenReturn(Optional.of(9093));
        //Curator mocks
        when(zkFactoryMock.get()).thenReturn(curatorMock);
        when(curatorMock.getChildren()).thenReturn(childrenBuilderMock);
        when(curatorMock.getData()).thenReturn(getDataBuilderMock);

        when(childrenBuilderMock.forPath(anyString())).thenReturn(List.of("broker-1", "broker-2"));
        when(getDataBuilderMock.forPath(contains("broker-1")))
                .thenReturn("{\"host\": \"192.168.1.100\", \"port\":9092}".getBytes("UTF-8"));
        when(getDataBuilderMock.forPath(contains("broker-2")))
                .thenReturn("{\"host\": \"192.168.1.101\", \"port\":9092}".getBytes("UTF-8"));

        final var kafkaLocationManager = new KafkaLocationManager(zkFactoryMock, kafkaSettingsMock);

        final var kafkaProperties = kafkaLocationManager.getProperties();

        Assertions.assertEquals("192.168.1.100:9093,192.168.1.101:9093",
                kafkaProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }

    @Test
    public void testItGetsTheListOfBrokersFromZookeeper() throws Exception {
        final var zkFactoryMock = mock(ZooKeeperHolder.class);
        final var kafkaSettingsMock = mock(KafkaSettings.class);
        final var curatorMock = mock(CuratorFramework.class);
        final var childrenBuilderMock = mock(GetChildrenBuilder.class);
        final var getDataBuilderMock = mock(GetDataBuilder.class);

        when(zkFactoryMock.get()).thenReturn(curatorMock);
        when(curatorMock.getChildren()).thenReturn(childrenBuilderMock);
        when(curatorMock.getData()).thenReturn(getDataBuilderMock);

        when(childrenBuilderMock.forPath(anyString())).thenReturn(List.of("broker-1", "broker-2"));
        when(getDataBuilderMock.forPath(contains("broker-1")))
                .thenReturn("{\"host\": \"192.168.1.100\", \"port\":9092}".getBytes("UTF-8"));
        when(getDataBuilderMock.forPath(contains("broker-2")))
                .thenReturn("{\"host\": \"192.168.1.101\", \"port\":9092}".getBytes("UTF-8"));

        final var kafkaLocationManager = new KafkaLocationManager(zkFactoryMock, kafkaSettingsMock);

        final var kafkaProperties = kafkaLocationManager.getProperties();

        Assertions.assertEquals("192.168.1.100:9092,192.168.1.101:9092",
                kafkaProperties.get(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG));
    }
}
