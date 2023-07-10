package org.zalando.nakadi.repository.kafka;

import com.codahale.metrics.MetricRegistry;
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
            super(null, new MetricRegistry(), numActiveProducers, consumerPoolSize);
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
        Assert.assertNotNull(producer1);

        final Producer<byte[], byte[]> producer2 = factory.takeProducer("topic-id");
        Assert.assertSame(producer1, producer2);
    }

    @Test
    public void testGoldenPathWithManyActiveProducers() {
        final KafkaFactory factory = new FakeKafkaFactory(4, 2);

        final List<Producer<byte[], byte[]>> producers = IntStream.range(0, 10)
                .mapToObj(ignore -> factory.takeProducer("topic-id")).collect(Collectors.toList());

        producers.forEach(Assert::assertNotNull);
    }
}
