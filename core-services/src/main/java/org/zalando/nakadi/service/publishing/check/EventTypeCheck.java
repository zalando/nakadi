package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;

import java.util.List;

public class EventTypeCheck extends Check {

    @Override
    public List<RecordResult> execute(final EventType eventType,
                                      final List<NakadiRecord> records) {

        for (final NakadiRecord record : records) {
            final String recordEventType = record.getMetadata().getEventType();

            if (!eventType.getName().equals(recordEventType)) {
                return processError(records, record, "event type does not match");
            }

            // todo should be amongst active schemas, fix after schema registry
//                final String recordSchemaVersion = record.getEventMetadata()
//                        .get("schema_version").toString();
//                if (!eventType.getSchema().getVersion().equals(recordSchemaVersion)) {
//
//                }

        }

        return null;
    }

    @Override
    public Step getCurrentStep() {
        return Step.VALIDATION;
    }

}
