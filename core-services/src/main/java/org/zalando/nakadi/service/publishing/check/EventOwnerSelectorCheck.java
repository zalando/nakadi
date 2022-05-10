package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventOwnerHeader;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.view.EventOwnerSelector;

import java.util.List;

public class EventOwnerSelectorCheck extends Check {

    private final AuthorizationValidator authValidator;

    public EventOwnerSelectorCheck(final AuthorizationValidator authValidator) {
        this.authValidator = authValidator;
    }

    @Override
    public List<NakadiRecordResult> execute(final EventType eventType,
                                            final List<NakadiRecord> records) {

        for (final NakadiRecord record : records) {
            final EventOwnerSelector eventOwnerSelector = eventType.getEventOwnerSelector();
            if (null == eventOwnerSelector) {
                continue;
            }

            try {
                switch (eventOwnerSelector.getType()) {
                    case STATIC:
                        record.setOwner(new EventOwnerHeader(
                                eventOwnerSelector.getName(),
                                eventOwnerSelector.getValue()));
                    case PATH:
                        // todo take from metadata (no field at the moment)
                    default:
                        record.setOwner(null);
                }

                authValidator.authorizeEventWrite(record);
            } catch (AccessDeniedException e) {
                return processError(records, record, e);
            }
        }

        return null;
    }


    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }
}
