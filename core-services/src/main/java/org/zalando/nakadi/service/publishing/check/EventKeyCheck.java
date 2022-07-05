package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;

import java.util.Collections;
import java.util.List;

public class EventKeyCheck extends Check {

    @Override
    public List<NakadiRecordResult> execute(final EventType eventType, final List<NakadiRecord> records) {

        for (final NakadiRecord record : records) {
            final String key = getEventKey(eventType, record.getMetadata());
            if (key != null) {
                record.setEventKey(key);
            }
        }
        return Collections.emptyList();
    }

    // FIXME: duplicated in EventPublisher
    private String getEventKey(final EventType eventType, final NakadiMetadata metadata) {

        if (eventType.getCleanupPolicy() == CleanupPolicy.COMPACT ||
                eventType.getCleanupPolicy() == CleanupPolicy.COMPACT_AND_DELETE) {

            return metadata.getPartitionCompactionKey();
        } else {
            final List<String> partitionKeys = metadata.getPartitionKeys();
            if (partitionKeys != null) {
                return String.join(",", partitionKeys);
            }
        }

        // that's fine, not all events get a key assigned to them
        return null;
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }
}
