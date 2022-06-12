package org.zalando.nakadi.service.publishing.check;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class Check {

    public abstract List<NakadiRecordResult> execute(EventType eventType, List<NakadiRecord> records);

    public abstract NakadiRecordResult.Step getCurrentStep();

    protected List<NakadiRecordResult> processError(
            final List<NakadiRecord> records,
            final NakadiRecord failedRecord,
            final Exception exception) {
        final List<NakadiRecordResult> recordResults = new LinkedList<>();
        if (failedRecord == null) { //This means all records are aborted due to some exception
            return records.stream()
                    .map(record -> new NakadiRecordResult(
                            record.getMetadata(),
                            NakadiRecordResult.Status.ABORTED,
                            getCurrentStep(),
                            exception))
                    .collect(Collectors.toList());
        }
        boolean metFailedRecord = false;
        for (final NakadiRecord nakadiRecord : records) {
            if (failedRecord == nakadiRecord) {
                recordResults.add(new NakadiRecordResult(
                        nakadiRecord.getMetadata(),
                        NakadiRecordResult.Status.FAILED,
                        getCurrentStep(),
                        exception));
                metFailedRecord = true;
            } else if (!metFailedRecord) {
                recordResults.add(new NakadiRecordResult(
                        nakadiRecord.getMetadata(),
                        NakadiRecordResult.Status.ABORTED,
                        getCurrentStep()));
            } else {
                recordResults.add(new NakadiRecordResult(
                        nakadiRecord.getMetadata(),
                        NakadiRecordResult.Status.ABORTED,
                        NakadiRecordResult.Step.NONE));
            }
        }

        return recordResults;
    }

}
