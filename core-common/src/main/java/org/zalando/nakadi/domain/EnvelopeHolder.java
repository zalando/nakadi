package org.zalando.nakadi.domain;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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
        writeIntToArray(data, 1, metadataLength);
        writeIntToArray(data, 1 + 4 + metadataLength, payloadLength);
        return data;
    }

    private static void writeIntToArray(
            final byte[] data,
            final int offset,
            final int value) {
        data[offset + 0] = (byte) (value >> 24);
        data[offset + 1] = (byte) (value >> 16);
        data[offset + 2] = (byte) (value >> 8);
        data[offset + 3] = (byte) (value >> 0);
    }

    public static EnvelopeHolder fromBytes(final byte[] data) throws IOException {
        final EnvelopeHolder envelopeHolder = new EnvelopeHolder();
        envelopeHolder.data = data;
        final ByteArrayInputStream bais = new ByteArrayInputStream(data);
        byte[] tmp = new byte[1];
        bais.read(tmp);
        envelopeHolder.metadataVersion = tmp[0];

        tmp = new byte[4];
        bais.read(tmp);
        envelopeHolder.metadataLength = toInt(tmp);

        bais.skip(envelopeHolder.metadataLength);
        tmp = new byte[4];
        bais.read(tmp);
        envelopeHolder.payloadLength = toInt(tmp);

        return envelopeHolder;
    }

    private static int toInt(final byte[] bytes) {
        return ((bytes[0] & 0xFF) << 24) |
                ((bytes[1] & 0xFF) << 16) |
                ((bytes[2] & 0xFF) << 8) |
                ((bytes[3] & 0xFF) << 0);
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
        return new ByteArrayInputStream(data, getPayloadOffset(), payloadLength);
    }

    public EventWriter getPayloadWriter() {
        return (outputStream) -> outputStream.write(data, getPayloadOffset(), payloadLength);
    }

    private int getPayloadOffset() {
        // metadata version = 1 byte
        // metadata length  = 4 byte (int)
        // actual metadata  = metadataLength
        // payload length   = 4 bytes (int)
        return 1 + 4 + metadataLength + 4;
    }
}
