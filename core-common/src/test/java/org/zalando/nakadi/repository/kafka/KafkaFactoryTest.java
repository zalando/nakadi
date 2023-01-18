package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KafkaFactoryTest {
    private static class FakeKafkaFactory extends KafkaFactory {

        FakeKafkaFactory() {
            super(null);
        }

        @Override
        protected Producer<byte[], byte[]> createProducerInstance(final String clientId) {
            return Mockito.mock(Producer.class);
        }
    }

    @Test
    public void whenSingleProducerThenTheSameProducerIsGiven() {
        final KafkaFactory factory = new FakeKafkaFactory();
        final Producer<byte[], byte[]> producer1 = factory.takeDefaultProducer();
        try {
            Assert.assertNotNull(producer1);
        } finally {
            factory.releaseProducer(producer1);
        }

        final Producer<byte[], byte[]> producer2 = factory.takeDefaultProducer();
        try {
            Assert.assertSame(producer1, producer2);
        } finally {
            factory.releaseProducer(producer2);
        }
    }

    @Test
    public void verifySingleProducerIsClosedAtCorrectTime() {
        final KafkaFactory factory = new FakeKafkaFactory();

        final List<Producer<byte[], byte[]>> producers1 = IntStream.range(0, 10)
                .mapToObj(ignore -> factory.takeDefaultProducer()).collect(Collectors.toList());
        final Producer<byte[], byte[]> producer = producers1.get(0);
        Assert.assertNotNull(producer);
        producers1.forEach(p -> Assert.assertSame(producer, p));
        producers1.forEach(factory::releaseProducer);

        Mockito.verify(producer, Mockito.times(0)).close();


        final List<Producer<byte[], byte[]>> producers2 = IntStream.range(0, 10)
                .mapToObj(ignore -> factory.takeDefaultProducer()).collect(Collectors.toList());
        final Producer<byte[], byte[]> additionalProducer = factory.takeDefaultProducer();

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
    public void verifyNewProducerCreatedAfterCloseOfSingle() {
        final KafkaFactory factory = new FakeKafkaFactory();
        final Producer<byte[], byte[]> producer1 = factory.takeDefaultProducer();
        Assert.assertNotNull(producer1);
        factory.terminateProducer(producer1);
        factory.releaseProducer(producer1);
        Mockito.verify(producer1, Mockito.times(1)).close();

        final Producer<byte[], byte[]> producer2 = factory.takeDefaultProducer();
        Assert.assertNotNull(producer2);
        Assert.assertNotSame(producer1, producer2);
        factory.releaseProducer(producer2);
        Mockito.verify(producer2, Mockito.times(0)).close();
    }

    @Test
    public void testTakingProducerForTheSameOrDifferentKey() {
        final KafkaFactory factory = new FakeKafkaFactory();

        final Producer<byte[], byte[]> producer1 = factory.takeProducer("key1");
        Assert.assertNotNull(producer1);

        final Producer<byte[], byte[]> producer2 = factory.takeProducer("key2");
        Assert.assertNotNull(producer2);

        Assert.assertNotSame(producer1, producer2);

        final Producer<byte[], byte[]> producer3 = factory.takeProducer("key1");
        Assert.assertNotNull(producer3);

        Assert.assertSame(producer3, producer1);

        factory.releaseProducer(producer1);
        factory.releaseProducer(producer2);
        factory.releaseProducer(producer3);
    }
}
