package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.exceptions.runtime.EventValidationException;

import java.util.Collections;
import java.util.List;

public class EventDeletionEmptyPayloadCheck extends Check {
    @Override
    public List<NakadiRecordResult> execute(final EventType eventType, final List<NakadiRecord> records) {
        for (final var record : records) {
            if (record.getPayload() != null && record.getPayload().length > 0) {
                return processError(records, record,
                        new EventValidationException("Payload must be empty to delete events."));
            }
        }
        return Collections.emptyList();
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }
}
