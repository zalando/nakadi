package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.publishing.EventOwnerExtractor;
import org.zalando.nakadi.service.publishing.EventOwnerExtractorFactory;

import java.util.List;

public class EventOwnerSelectorCheck extends Check {

    private final EventOwnerExtractorFactory eventOwnerExtractorFactory;
    private final AuthorizationValidator authValidator;

    public EventOwnerSelectorCheck(final EventOwnerExtractorFactory eventOwnerExtractorFactory,
                                   final AuthorizationValidator authValidator) {
        this.eventOwnerExtractorFactory = eventOwnerExtractorFactory;
        this.authValidator = authValidator;
    }

    @Override
    public List<RecordResult> execute(final EventType eventType,
                                      final List<NakadiRecord> records) {

        final EventOwnerExtractor extractor = eventOwnerExtractorFactory.createExtractor(eventType);
        if (null == extractor) {
            return null; // means has no selector or feature disabled
        }

        for (final NakadiRecord record : records) {
            record.setOwner(extractor.extractEventOwner(record.getMetadata()));
            try {
                authValidator.authorizeEventWrite(record);
            } catch (AccessDeniedException e) {
                return processError(records, record, e.explain());
            }
        }

        return null;
    }

    @Override
    public Step getCurrentStep() {
        return Step.VALIDATION;
    }
}
