package org.zalando.nakadi.repository.kafka;

import org.apache.kafka.clients.producer.Producer;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventPublishingStatus;

public class ProducerSendCommandTest {

    @Test
    public void testCommand() throws Exception {
        final BatchItem batchItem = new BatchItem(new JSONObject());
        batchItem.setTopic("topic");
        batchItem.setPartition("0");

        final Producer kafkaProducer = Mockito.mock(Producer.class);
        Mockito.when(kafkaProducer.send(Mockito.any(), Mockito.any())).thenThrow(new RuntimeException());

        new ProducerSendCommand(kafkaProducer, batchItem).execute();

        Assert.assertTrue(batchItem.getResponse().getPublishingStatus() == EventPublishingStatus.FAILED);
        Assert.assertTrue(batchItem.getResponse().getDetail().equals("internal error"));
    }

}