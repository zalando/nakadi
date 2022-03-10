package org.zalando.nakadi.repository.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.Queue;

public class SequenceDecoder {
    private final InputStreamEnumeration inputStreamEnumeration = new InputStreamEnumeration();
    private final DatumReader<GenericRecord> datumReader;
    private final BinaryDecoder binaryDecoder;

    private static class InputStreamEnumeration implements Enumeration<InputStream> {
        private final Queue<InputStream> inputStreams = new LinkedList<>();

        @Override
        public boolean hasMoreElements() {
            return !this.inputStreams.isEmpty();
        }

        @Override
        public InputStream nextElement() {
            return this.inputStreams.remove();
        }

        public void add(final InputStream inputStream) {
            inputStreams.add(inputStream);
        }
    }

    public SequenceDecoder(final Schema schema) {
        this.datumReader = new GenericDatumReader<>(schema);
        //Adding an empty stream so that Building SequenceInputStream won't close it immediately.
        inputStreamEnumeration.add(new ByteArrayInputStream(new byte[0]));
        this.binaryDecoder = DecoderFactory.get()
                .directBinaryDecoder(new SequenceInputStream(inputStreamEnumeration), null);
    }

    public GenericRecord read(final InputStream inputStream) throws IOException {
        inputStreamEnumeration.add(inputStream);
        return datumReader.read(null, binaryDecoder);
    }
}
