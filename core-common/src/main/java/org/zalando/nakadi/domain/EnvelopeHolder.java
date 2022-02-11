package org.zalando.nakadi.domain;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public class EnvelopeHolder {

    private static final byte[] FOUR_BYTE_ARRAY = new byte[4];
    private byte metadataVersion;
    private int metadataLength;
    private int payloadLength;
    private byte[] data;

    public interface EventWriter {
        void write(OutputStream os) throws IOException;
    }

    public static byte[] produceBytes(
            final byte metadataVersion,
            final EventWriter metadataWriter,
            final EventWriter eventWriter) throws IOException {
        final ByteArrayOutputStream eventOutputStream = new ByteArrayOutputStream();
        eventOutputStream.write(metadataVersion);
        eventOutputStream.write(FOUR_BYTE_ARRAY);
        metadataWriter.write(eventOutputStream);
        final int metadataLength = eventOutputStream.size() - 1 - 4;
        eventOutputStream.write(FOUR_BYTE_ARRAY);
        eventWriter.write(eventOutputStream);
        final int payloadLength = eventOutputStream.size() - 1 - 4 - metadataLength - 4;

        final byte[] data = eventOutputStream.toByteArray();

        final ByteBuffer bb = ByteBuffer.wrap(data);
        bb.put(metadataVersion);
        bb.putInt(metadataLength);
        bb.position(bb.position() + metadataLength);
        bb.putInt(payloadLength);
        return data;
    }

    public static EnvelopeHolder fromBytes(final byte[] data) throws IOException {
        final EnvelopeHolder envelopeHolder = new EnvelopeHolder();
        envelopeHolder.data = data;
        final ByteBuffer bb = ByteBuffer.wrap(data);
        envelopeHolder.metadataVersion = bb.get();
        envelopeHolder.metadataLength = bb.getInt();
        // skip metadata
        bb.position(bb.position() + envelopeHolder.metadataLength);
        envelopeHolder.payloadLength = bb.getInt();
        return envelopeHolder;
    }


    private EnvelopeHolder() {
    }

    public byte getMetadataVersion() {
        return metadataVersion;
    }

    public InputStream getMetadata() {
        return new ByteArrayInputStream(data, 1 + 4, metadataLength);
    }

    public InputStream getPayload() {
        return new ByteArrayInputStream(data, 1 + 4 + metadataLength + 4, payloadLength);
    }

}
