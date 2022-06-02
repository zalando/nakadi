package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.exceptions.runtime.EventValidationException;

import java.util.Collections;
import java.util.List;

public class EventDeletionCleanupPolicyCheck extends Check {
    @Override
    public List<NakadiRecordResult> execute(
            final EventType eventType,
            final List<NakadiRecord> records, final boolean delete) {
        if (eventType.getCleanupPolicy() == CleanupPolicy.DELETE) {
            processError(records, null,
                    new EventValidationException("It is not allowed to delete events from non compacted event type"));
        }
        return Collections.emptyList();
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }
}
