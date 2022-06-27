package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;

import java.util.Collections;
import java.util.List;

public class EventSizeCheck extends Check {
    @Override
    public List<NakadiRecordResult> execute(final EventType eventType, final List<NakadiRecord> records) {
        // TODO - team-aruha/issues/1136 - Implement event size check for avro events
        return Collections.emptyList();
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }
}
