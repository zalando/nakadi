package org.zalando.nakadi.domain;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class Envelop {

    private byte metadataVersion;
    private byte[] metadataLength;
    private byte[] metadata;
    private byte[] payloadLength;
    private byte[] payload;

    public static byte[] serialize(final byte metadataVersion,
                                   final int metadataLength,
                                   final byte[] metadata,
                                   final int payloadLength,
                                   final byte[] payload) {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(metadataVersion);
        baos.writeBytes(toBytes(metadataLength));
        baos.writeBytes(metadata);
        baos.writeBytes(toBytes(payloadLength));
        baos.writeBytes(payload);

        return baos.toByteArray();
    }

    private static byte[] toBytes(final int value) {
        return new byte[]{
                (byte) (value >> 24),
                (byte) (value >> 16),
                (byte) (value >> 8),
                (byte) value};
    }

    public static Envelop deserialize(final byte[] data) throws IOException {
        final Envelop envelop = new Envelop();
        final ByteArrayInputStream bais = new ByteArrayInputStream(data);
        byte[] tmp = new byte[1];
        bais.read(tmp);
        envelop.metadataVersion = tmp[0];

        tmp = new byte[4];
        bais.read(tmp);
        envelop.metadataLength = tmp;

        tmp = new byte[toInt(tmp)];
        bais.read(tmp);
        envelop.metadata = tmp;

        tmp = new byte[4];
        bais.read(tmp);
        envelop.payloadLength = tmp;

        tmp = new byte[toInt(tmp)];
        bais.read(tmp);
        envelop.payload = tmp;

        return envelop;
    }

    private static int toInt(final byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                ((bytes[3] & 0xFF) << 0);
    }

    private Envelop() {
    }

    public byte getMetadataVersion() {
        return metadataVersion;
    }

    public byte[] getMetadataLength() {
        return metadataLength;
    }

    public byte[] getMetadata() {
        return metadata;
    }

    public byte[] getPayloadLength() {
        return payloadLength;
    }

    public byte[] getPayload() {
        return payload;
    }

}
