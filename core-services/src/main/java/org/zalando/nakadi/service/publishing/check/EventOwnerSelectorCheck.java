package org.zalando.nakadi.service.publishing.check;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.EventOwnerExtractionException;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.publishing.EventOwnerExtractor;
import org.zalando.nakadi.service.publishing.EventOwnerExtractorFactory;

import java.util.Collections;
import java.util.List;

public class EventOwnerSelectorCheck extends Check {

    private static final Logger LOG = LoggerFactory.getLogger(EventOwnerSelectorCheck.class);

    private final EventOwnerExtractorFactory eventOwnerExtractorFactory;
    private final AuthorizationValidator authValidator;

    public EventOwnerSelectorCheck(final EventOwnerExtractorFactory eventOwnerExtractorFactory,
                                   final AuthorizationValidator authValidator) {
        this.eventOwnerExtractorFactory = eventOwnerExtractorFactory;
        this.authValidator = authValidator;
    }

    @Override
    public List<NakadiRecordResult> execute(final EventType eventType, final List<NakadiRecord> records) {

        final EventOwnerExtractor extractor = eventOwnerExtractorFactory.createExtractor(eventType);
        if (null == extractor) {
            return null; // means has no selector or feature disabled
        }

        for (final NakadiRecord record : records) {
            try {
                record.setOwner(extractor.extractEventOwner(record.getMetadata()));
            } catch (final EventOwnerExtractionException e) {
                LOG.warn("Failed to extract event owner for {}: {}", eventType.getName(), e.getMessage());

                // return processError(records, record, e);
            }
            try {
                authValidator.authorizeEventWrite(record);
            } catch (final AccessDeniedException e) {
                return processError(records, record, e);
            }
        }

        return Collections.emptyList();
    }

    @Override
    public NakadiRecordResult.Step getCurrentStep() {
        return NakadiRecordResult.Step.VALIDATION;
    }
}
