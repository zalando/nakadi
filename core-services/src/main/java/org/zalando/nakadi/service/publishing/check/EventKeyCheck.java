package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;

import java.util.List;

public class EventKeyCheck extends Check {

    @Override
    public List<RecordResult> execute(final EventType eventType,
                                      final List<NakadiRecord> records) {
        return null;
    }

    @Override
    public Step getCurrentStep() {
        return null;
    }
}
