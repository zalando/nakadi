package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;

import java.util.List;

public class EventTypeCheck extends Check {

    @Override
    public List<NakadiRecordResult> execute(final EventType eventType,
                                            final List<NakadiRecord> records) {

        for (final NakadiRecord record : records) {
            final String recordEventType = record.getMetadata().getEventType();

            if (!eventType.getName().equals(recordEventType)) {
                return processError(records, record,
                        new InvalidEventTypeException("event type does not match"));
            }

            // todo should be amongst active schemas, fix after schema registry
//                final String recordSchemaVersion = record.getEventMetadata()
//                        .get("version").toString();
//                if (!eventType.getSchema().getVersion().equals(recordSchemaVersion)) {
//
//                }

        }

        return null;
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }

}
