package org.zalando.nakadi;

import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.BatchItemResponse;
import org.zalando.nakadi.domain.EventPublishResult;
import org.zalando.nakadi.domain.EventPublishingStatus;
import org.zalando.nakadi.domain.EventPublishingStep;
import org.zalando.nakadi.domain.NakadiRecordResult;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class PublishingResultConverter {

    public EventPublishResult mapPublishingResultToView(final List<NakadiRecordResult> recordsMetadata) {
        final List<BatchItemResponse> batchItemResponses = new LinkedList<>();
        for (final NakadiRecordResult recordMetadata : recordsMetadata) {
            final EventPublishingStatus status = mapPublishingStatus(recordMetadata.getStatus());
            batchItemResponses.add(new BatchItemResponse()
                    .setStep(EventPublishingStep.PUBLISHING)
                    .setPublishingStatus(status)
                    .setEid(recordMetadata.getMetadata().getEid())
                    .setDetail((recordMetadata.getException() != null) ?
                            recordMetadata.getException().getMessage() : ""));
        }

        final var overallStatus = getOverallStatus(batchItemResponses);
        return new EventPublishResult(overallStatus, EventPublishingStep.PUBLISHING, batchItemResponses);
    }

    private EventPublishingStatus getOverallStatus(final List<BatchItemResponse> batchItemResponses) {
        final var publishingStatusSet = batchItemResponses.stream()
                .map(BatchItemResponse::getPublishingStatus)
                .collect(Collectors.toSet());

        if (publishingStatusSet.contains(EventPublishingStatus.FAILED)) {
            return EventPublishingStatus.FAILED;
        } else if (publishingStatusSet.contains(EventPublishingStatus.ABORTED)) {
            return EventPublishingStatus.ABORTED;
        } else {
            return EventPublishingStatus.SUBMITTED;
        }
    }

    private EventPublishingStatus mapPublishingStatus(final NakadiRecordResult.Status status) {
        switch (status) {
            case FAILED:
                return EventPublishingStatus.FAILED;
            case SUCCEEDED:
                return EventPublishingStatus.SUBMITTED;
            case ABORTED:
                return EventPublishingStatus.ABORTED;
            default:
                throw new RuntimeException(String.format(
                        "publishing status from record is not defined: `%s`", status));
        }
    }

}
