package org.zalando.nakadi.repository.kafka;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.repository.kafka.KafkaFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KafkaFactoryTest {
    private static class FakeKafkaFactory extends KafkaFactory {

        FakeKafkaFactory(final MetricRegistry metricRegistry) {
            super(null, metricRegistry);
        }

        @Override
        protected Producer<String, String> createProducerInstance() {
            return Mockito.mock(Producer.class);
        }
    }

    private static KafkaFactory createTestKafkaFactory() {
        final MetricRegistry reg = Mockito.mock(MetricRegistry.class);
        Mockito.when(reg.counter(Mockito.anyString())).thenReturn(Mockito.mock(Counter.class));
        return new FakeKafkaFactory(reg);
    }

    @Test
    public void verifySameProducerUsed() {
        final KafkaFactory factory = createTestKafkaFactory();
        final Producer<String, String> producer1 = factory.takeProducer();
        try {
            Assert.assertNotNull(producer1);
        } finally {
            factory.releaseProducer(producer1);
        }

        final Producer<String, String> producer2 = factory.takeProducer();
        try {
            Assert.assertSame(producer1, producer2);
        } finally {
            factory.releaseProducer(producer2);
        }
    }

    @Test
    public void verifyProducerIsClosedAtCorrectTime() {
        final KafkaFactory factory = createTestKafkaFactory();

        final List<Producer<String, String>> producers1 = IntStream.range(0, 10)
                .mapToObj(ignore -> factory.takeProducer()).collect(Collectors.toList());
        final Producer<String, String> producer = producers1.get(0);
        Assert.assertNotNull(producer);
        producers1.forEach(p -> Assert.assertSame(producer, p));
        producers1.forEach(factory::releaseProducer);

        Mockito.verify(producer, Mockito.times(0)).close();


        final List<Producer<String, String>> producers2 = IntStream.range(0, 10)
                .mapToObj(ignore -> factory.takeProducer()).collect(Collectors.toList());
        final Producer<String, String> additionalProducer = factory.takeProducer();

        Assert.assertSame(producer, additionalProducer);
        producers2.forEach(p -> Assert.assertSame(producer, p));

        factory.terminateProducer(producers2.get(0));
        Mockito.verify(producer, Mockito.times(0)).close();

        producers2.forEach(factory::releaseProducer);
        Mockito.verify(producer, Mockito.times(0)).close();

        factory.releaseProducer(additionalProducer);
        Mockito.verify(producer, Mockito.times(1)).close();
    }

    @Test
    public void verifyNewProducerCreatedAfterClose() {
        final KafkaFactory factory = createTestKafkaFactory();
        final Producer<String, String> producer1 = factory.takeProducer();
        Assert.assertNotNull(producer1);
        factory.terminateProducer(producer1);
        factory.releaseProducer(producer1);
        Mockito.verify(producer1, Mockito.times(1)).close();

        final Producer<String, String> producer2 = factory.takeProducer();
        Assert.assertNotNull(producer2);
        Assert.assertNotSame(producer1, producer2);
        factory.releaseProducer(producer2);
        Mockito.verify(producer2, Mockito.times(0)).close();
    }
}