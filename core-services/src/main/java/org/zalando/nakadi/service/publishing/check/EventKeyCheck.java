package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;

import java.util.Collections;
import java.util.List;

public class EventKeyCheck extends Check {

    @Override
    public List<NakadiRecordResult> execute(final EventType eventType, final List<NakadiRecord> records) {
        return Collections.emptyList();
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }
}
