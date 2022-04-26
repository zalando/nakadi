package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;

import java.util.List;
import java.util.stream.Collectors;

public abstract class Check {

    public abstract List<RecordResult> execute(
            final EventType eventType,
            final List<NakadiRecord> records);

    public abstract Step getCurrentStep();

    protected List<RecordResult> processError(
            final List<NakadiRecord> records,
            final NakadiRecord failedRecord,
            final String error) {
        return records.stream().map(nakadiRecord -> new RecordResult(
                failedRecord == nakadiRecord ? Status.FAILED : Status.ABORTED,
                nakadiRecord.getMetadata().getEid(),
                error)
        ).collect(Collectors.toList());
    }

    public class RecordResult {
        private final Status status;
        private final Step step;
        private final String eid;
        private final String error;

        public RecordResult(final Status status,
                            final String eid,
                            final String error) {
            this.status = status;
            this.eid = eid;
            this.error = error;
            this.step = getCurrentStep();
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
        VALIDATION,
        ENRICHMENT,
        PARTITIONING
    }
}
