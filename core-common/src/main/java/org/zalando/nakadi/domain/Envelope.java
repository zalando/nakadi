package org.zalando.nakadi.domain;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Envelope {

    private byte metadataVersion;
    private int metadataLength;
    private byte[] metadata;
    private int payloadLength;
    private byte[] payload;

    public static byte[] serialize(final Envelope envelope) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(envelope.metadataVersion);
        baos.writeBytes(toBytes(envelope.metadataLength));
        baos.writeBytes(envelope.metadata);
        baos.writeBytes(toBytes(envelope.payloadLength));
        baos.writeBytes(envelope.payload);

        return baos.toByteArray();
    }

    private static byte[] toBytes(final int value) {
        return new byte[]{
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) value};
    }

    public static Envelope deserialize(final byte[] data) throws IOException {
        final Envelope envelope = new Envelope();
        final ByteArrayInputStream bais = new ByteArrayInputStream(data);
        byte[] tmp = new byte[1];
        bais.read(tmp);
        envelope.metadataVersion = tmp[0];

        tmp = new byte[4];
        bais.read(tmp);
        envelope.metadataLength = toInt(tmp);

        tmp = new byte[envelope.metadataLength];
        bais.read(tmp);
        envelope.metadata = tmp;

        tmp = new byte[4];
        bais.read(tmp);
        envelope.payloadLength = toInt(tmp);

        tmp = new byte[envelope.payloadLength];
        bais.read(tmp);
        envelope.payload = tmp;

        return envelope;
    }

    private static int toInt(final byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                ((bytes[3] & 0xFF) << 0);
    }

    private Envelope() {
    }

    public Envelope(final byte metadataVersion,
                    final int metadataLength,
                    final byte[] metadata,
                    final int payloadLength,
                    final byte[] payload) {
        this.metadataVersion = metadataVersion;
        this.metadataLength = metadataLength;
        this.metadata = metadata;
        this.payloadLength = payloadLength;
        this.payload = payload;
    }

    public byte[] getMetadata() {
        return metadata;
    }

    public byte[] getPayload() {
        return payload;
    }

}
