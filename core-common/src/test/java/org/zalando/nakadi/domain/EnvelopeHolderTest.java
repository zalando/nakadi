package org.zalando.nakadi.domain;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class EnvelopeHolderTest {

    @Test
    public void testProduceBytes() throws IOException {
        final String metadata = "metadata is cool";
        final String payload = "payload is huge";

        final byte[] data = EnvelopeHolder.produceBytes((byte) 0,
                (os) -> os.write(metadata.getBytes(StandardCharsets.UTF_8)),
                (os) -> os.write(payload.getBytes(StandardCharsets.UTF_8)));

        final EnvelopeHolder envelopeHolder = EnvelopeHolder.fromBytes(data);
        Assert.assertEquals(metadata, new String(envelopeHolder.getMetadata().readAllBytes()));
        Assert.assertEquals(payload, new String(envelopeHolder.getPayload().readAllBytes()));
    }
}