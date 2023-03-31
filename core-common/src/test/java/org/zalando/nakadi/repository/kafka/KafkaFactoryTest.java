package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class KafkaFactoryTest {
    private static class FakeKafkaFactory extends KafkaFactory {

        FakeKafkaFactory(final int numActiveProducers, final int consumerPoolSize) {
            super(null, numActiveProducers, consumerPoolSize);
        }

        @Override
        protected Producer<byte[], byte[]> createProducerInstance() {
            return Mockito.mock(Producer.class);
        }

        @Override
        protected Consumer<byte[], byte[]> createConsumerProxyInstance() {
            return Mockito.mock(Consumer.class);
        }
    }

    @Test
    public void whenSingleProducerThenTheSameProducerIsGiven() {
        final KafkaFactory factory = new FakeKafkaFactory(1, 2);
        final Producer<byte[], byte[]> producer1 = factory.takeProducer("topic-id");
        try {
            Assert.assertNotNull(producer1);
        } finally {
            factory.releaseProducer(producer1);
        }

        final Producer<byte[], byte[]> producer2 = factory.takeProducer("topic-id");
        try {
            Assert.assertSame(producer1, producer2);
        } finally {
            factory.releaseProducer(producer2);
        }
    }

    @Test
    public void verifySingleProducerIsClosedAtCorrectTime() {
        final KafkaFactory factory = new FakeKafkaFactory(1, 2);

        final List<Producer<byte[], byte[]>> producers1 = IntStream.range(0, 10)
                .mapToObj(ignore -> factory.takeProducer("topic-id")).collect(Collectors.toList());
        final Producer<byte[], byte[]> producer = producers1.get(0);
        Assert.assertNotNull(producer);
        producers1.forEach(p -> Assert.assertSame(producer, p));
        producers1.forEach(factory::releaseProducer);

        Mockito.verify(producer, Mockito.times(0)).close();


        final List<Producer<byte[], byte[]>> producers2 = IntStream.range(0, 10)
                .mapToObj(ignore -> factory.takeProducer("topic-id")).collect(Collectors.toList());
        final Producer<byte[], byte[]> additionalProducer = factory.takeProducer("topic-id");

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
        final KafkaFactory factory = new FakeKafkaFactory(1, 2);
        final Producer<byte[], byte[]> producer1 = factory.takeProducer("topic-id");
        Assert.assertNotNull(producer1);
        factory.terminateProducer(producer1);
        factory.releaseProducer(producer1);
        Mockito.verify(producer1, Mockito.times(1)).close();

        final Producer<byte[], byte[]> producer2 = factory.takeProducer("topic-id");
        Assert.assertNotNull(producer2);
        Assert.assertNotSame(producer1, producer2);
        factory.releaseProducer(producer2);
        Mockito.verify(producer2, Mockito.times(0)).close();
    }

    @Test
    public void testGoldenPathWithManyActiveProducers() {
        final KafkaFactory factory = new FakeKafkaFactory(4, 2);

        final List<Producer<byte[], byte[]>> producers = IntStream.range(0, 10)
                .mapToObj(ignore -> factory.takeProducer("topic-id")).collect(Collectors.toList());

        producers.forEach(Assert::assertNotNull);
        producers.forEach(factory::releaseProducer);
    }
}
