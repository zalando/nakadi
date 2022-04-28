package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;

import java.util.LinkedList;
import java.util.List;

public abstract class Check {

    public abstract List<RecordResult> execute(
            EventType eventType,
            List<NakadiRecord> records);

    public abstract Step getCurrentStep();

    protected List<RecordResult> processError(
            final List<NakadiRecord> records,
            final NakadiRecord failedRecord,
            final String error) {
        final List<RecordResult> recordResults = new LinkedList<>();
        boolean metFailedRecord = false;
        for (final NakadiRecord nakadiRecord : records) {
            if (failedRecord == nakadiRecord) {
                recordResults.add(new RecordResult(
                        Status.FAILED,
                        nakadiRecord.getMetadata().getEid(),
                        error));
                metFailedRecord = true;
            } else if (!metFailedRecord) {
                recordResults.add(new RecordResult(
                        Status.ABORTED,
                        nakadiRecord.getMetadata().getEid(),
                        ""));
            } else {
                recordResults.add(new RecordResult(
                        Status.ABORTED,
                        Step.NONE,
                        nakadiRecord.getMetadata().getEid(),
                        ""));
            }
        }

        return recordResults;
    }

    public class RecordResult {
        private final Status status;
        private final Step step;
        private final String eid;
        private final String error;

        public RecordResult(final Status status,
                            final Step step,
                            final String eid,
                            final String error) {
            this.status = status;
            this.step = step;
            this.eid = eid;
            this.error = error;
        }

        public RecordResult(final Status status,
                            final String eid,
                            final String error) {
            this(status, getCurrentStep(), eid, error);
        }

        public Status getStatus() {
            return status;
        }

        public Step getStep() {
            return step;
        }

        public String getEid() {
            return eid;
        }

        public String getError() {
            return error;
        }
    }

    public enum Status {
        FAILED, ABORTED
    }

    public enum Step {
        NONE,
        // TODO : Maybe reuse EventPublishingStep
        VALIDATION,
        ENRICHMENT,
        PARTITIONING
    }
}
