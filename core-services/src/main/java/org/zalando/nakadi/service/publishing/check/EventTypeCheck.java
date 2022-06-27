package org.zalando.nakadi.service.publishing.check;

import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.service.SchemaService;

import java.util.Collections;
import java.util.List;

@Component
public class EventTypeCheck extends Check {

    private final SchemaService schemaService;

    public EventTypeCheck(final SchemaService schemaService) {
        this.schemaService = schemaService;
    }

    @Override
    public List<NakadiRecordResult> execute(final EventType eventType, final List<NakadiRecord> records) {

        for (final NakadiRecord record : records) {
            final NakadiMetadata metadata = record.getMetadata();
            final String recordEventType = metadata.getEventType();

            if (!eventType.getName().equals(recordEventType)) {
                return processError(records, record,
                        new InvalidEventTypeException("event type does not match"));
            }

            try {
                // fixme: potential for DoS by sending non-existing version constantly
                schemaService.getSchemaVersion(recordEventType, metadata.getSchemaVersion());
            } catch (final Exception ex) {
                return processError(records, record, ex);
            }
        }

        return Collections.emptyList();
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }

}
