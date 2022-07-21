package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.partitioning.EventKeyExtractor;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class EventKeyCheck extends Check {

    @Override
    public List<NakadiRecordResult> execute(final EventType eventType, final List<NakadiRecord> records) {
        final Function<NakadiMetadata, String> kafkaKeyExtractor =
                EventKeyExtractor.kafkaKeyFromNakadiMetadata(eventType);

        for (final NakadiRecord record : records) {
            record.setEventKey(kafkaKeyExtractor.apply(record.getMetadata()));
        }
        return Collections.emptyList();
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }
}
