package org.zalando.nakadi;

import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.BatchItemResponse;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventPublishingStep;
import org.zalando.nakadi.domain.NakadiRecordResult;
import org.zalando.nakadi.service.publishing.check.Check;

import java.util.LinkedList;
import java.util.List;

@Service
public class PublishingResultConverter {

    public EventPublishResult mapCheckResultToView(final List<Check.RecordResult> recordResults) {
        final List<BatchItemResponse> batchItemResponses = new LinkedList<>();
        EventPublishingStep step = null;
        EventPublishingStatus status = null;
        for (final Check.RecordResult recordResult : recordResults) {
            status = mapPublishingStatus(recordResult.getStatus());
            step = mapPublishingStep(recordResult.getStep());
            batchItemResponses.add(new BatchItemResponse()
                    .setPublishingStatus(status)
                    .setEid(recordResult.getEid())
                    .setDetail(recordResult.getError())
                    .setStep(step));
        }

        return new EventPublishResult(status, step, batchItemResponses);
    }

    private EventPublishingStep mapPublishingStep(final Check.Step step) {
        switch (step) {
            case NONE:
                return EventPublishingStep.NONE;
            case ENRICHMENT:
                return EventPublishingStep.ENRICHING;
            case VALIDATION:
                return EventPublishingStep.VALIDATING;
            case PARTITIONING:
                return EventPublishingStep.PARTITIONING;
            default:
                throw new RuntimeException("publishing step is not defined");
        }
    }

    private EventPublishingStatus mapPublishingStatus(final Check.Status status) {
        switch (status) {
            case FAILED:
                return EventPublishingStatus.FAILED;
            case ABORTED:
                return EventPublishingStatus.ABORTED;
            default:
                throw new RuntimeException(String.format(
                        "publishing status from check is not defined: `%s`", status));
        }
    }

    public EventPublishResult mapPublishingResultToView(final List<NakadiRecordResult> recordsMetadata) {
        final List<BatchItemResponse> batchItemResponses = new LinkedList<>();
        EventPublishingStatus status = null;
        for (final NakadiRecordResult recordMetadata : recordsMetadata) {
            status = mapPublishingStatus(recordMetadata.getStatus());
            batchItemResponses.add(new BatchItemResponse()
                    .setStep(EventPublishingStep.PUBLISHING)
                    .setPublishingStatus(mapPublishingStatus(recordMetadata.getStatus()))
                    .setEid(recordMetadata.getMetadata().getEid())
                    .setDetail(recordMetadata.getException().getMessage()));
        }

        return new EventPublishResult(status, EventPublishingStep.PUBLISHING, batchItemResponses);
    }

    private EventPublishingStatus mapPublishingStatus(final NakadiRecordResult.Status status) {
        switch (status) {
            case FAILED:
                return EventPublishingStatus.FAILED;
            case SUCCEEDED:
                return EventPublishingStatus.SUBMITTED;
            case NOT_ATTEMPTED:
                return EventPublishingStatus.ABORTED;
            default:
                throw new RuntimeException(String.format(
                        "publishing status from record is not defined: `%s`", status));
        }
    }

}
