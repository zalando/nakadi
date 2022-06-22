package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;

import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class EventKeyCheck extends Check {

    @Override
    public List<NakadiRecordResult> execute(final EventType eventType, final List<NakadiRecord> records) {
        for (final var record : records) {
            final var partitionCompactionKey = record.getMetadata().getPartitionCompactionKey();
            if (partitionCompactionKey != null) {
                record.setEventKey(partitionCompactionKey.getBytes(StandardCharsets.UTF_8));
            }
        }
        return Collections.emptyList();
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }
}
