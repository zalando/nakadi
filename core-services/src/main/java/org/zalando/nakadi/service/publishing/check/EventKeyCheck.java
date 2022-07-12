package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.partitioning.PartitionResolver;

import java.util.Collections;
import java.util.List;

public class EventKeyCheck extends Check {

    @Override
    public List<NakadiRecordResult> execute(final EventType eventType, final List<NakadiRecord> records) {

        for (final NakadiRecord record : records) {
            final String key = PartitionResolver.getEventKey(eventType, record.getMetadata());
            if (key != null) {
                record.setEventKey(key);
            }
        }
        return Collections.emptyList();
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }
}
