package org.zalando.nakadi.service;

import com.google.common.io.CountingOutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.generated.avro.ConsumptionBatch;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.SubscriptionCursor;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class EventStreamBinaryWriter implements EventStreamWriter {

    private final NakadiRecordMapper nakadiRecordMapper;

    @Autowired
    public EventStreamBinaryWriter(final NakadiRecordMapper nakadiRecordMapper) {
        this.nakadiRecordMapper = nakadiRecordMapper;
    }

    @Override
    public int writeBatch(final OutputStream os, final Cursor cursor, final List<byte[]> events) throws IOException {
        throw new RuntimeException();
    }

    @Override
    public int writeSubscriptionBatch(final OutputStream os,
                                      final SubscriptionCursor cursor,
                                      final List<ConsumedEvent> events,
                                      final Optional<String> metadata) throws IOException {
        final ConsumptionBatch batch = ConsumptionBatch.newBuilder()
                .setCursor(org.zalando.nakadi.generated.avro.SubscriptionCursor.newBuilder()
                        .setEventType(cursor.getEventType())
                        .setOffset(cursor.getOffset())
                        .setPartition(cursor.getPartition())
                        .setCursorToken(cursor.getCursorToken())
                        .build())
                .setInfo(metadata.orElse(null))
                .setEvents(events.stream()
                        .map(ConsumedEvent::getEvent)
                        .map(nakadiRecordMapper::fromBytesEnvelope)
                        .collect(Collectors.toList())
                )
                .build();

        final CountingOutputStream countingOutputStream = new CountingOutputStream(os);
        ConsumptionBatch.getEncoder().encode(batch, countingOutputStream);

        return (int) countingOutputStream.getCount();
    }
}
