package org.zalando.nakadi.service.publishing.check;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResult;

import java.util.List;

public class CheckTest {

    @Test
    public void testAbortRecordsBeforeAndAfterFailedRecord() {
        final NakadiRecord recordOne = new NakadiRecord().setMetadata(nakadiMetadata);
        final NakadiRecord failedRecord = new NakadiRecord().setMetadata(nakadiMetadata);
        final NakadiRecord recordThree = new NakadiRecord().setMetadata(nakadiMetadata);
        final List<NakadiRecord> records = Lists.newArrayList(
                recordOne, failedRecord, recordThree);

        final List<NakadiRecordResult> results =
                check.processError(records, failedRecord, new RuntimeException("test-error"));
        Assert.assertEquals(NakadiRecordResult.Status.ABORTED, results.get(0).getStatus());
        Assert.assertEquals(NakadiRecordResult.Step.VALIDATION, results.get(0).getStep());
        Assert.assertNull(results.get(0).getException());

        Assert.assertEquals(NakadiRecordResult.Status.FAILED, results.get(1).getStatus());
        Assert.assertEquals(NakadiRecordResult.Step.VALIDATION, results.get(1).getStep());
        Assert.assertNotNull(results.get(1).getException());

        Assert.assertEquals(NakadiRecordResult.Status.ABORTED, results.get(2).getStatus());
        Assert.assertEquals(NakadiRecordResult.Step.NONE, results.get(2).getStep());
        Assert.assertNull(results.get(2).getException());
    }

    private final Check check = new Check() {
        @Override
        public List<NakadiRecordResult> execute(final EventType eventType,
                                                final List<NakadiRecord> records) {
            return null;
        }

        @Override
        public NakadiRecordResult.Step getCurrentStep() {
            return NakadiRecordResult.Step.VALIDATION;
        }
    };

    private final NakadiMetadata nakadiMetadata = new NakadiMetadata() {
        @Override
        public String getEid() {
            return "12345";
        }

        @Override
        public long getOccurredAt() {
            return 0L;
        }

        @Override
        public String getPartitionStr() {
            return null;
        }

        @Override
        public Integer getPartitionInt() {
            return null;
        }

        @Override
        public void setPartition(final String partition) {

        }

        @Override
        public String getPublishedBy() {
            return null;
        }

        @Override
        public void setPublishedBy(final String publisher) {

        }

        @Override
        public long getReceivedAt() {
            return 0L;
        }

        @Override
        public void setReceivedAt(final long receivedAt) {

        }

        @Override
        public String getFlowId() {
            return null;
        }

        @Override
        public void setFlowId(final String flowId) {

        }

        @Override
        public String getSchemaVersion() {
            return null;
        }

        @Override
        public void setSchemaVersion(final String toString) {

        }

        @Override
        public List<String> getPartitionKeys() {
            return null;
        }

        @Override
        public void setPartitionKeys(final List<String> partitionKeys) {

        }

        @Override
        public String getPartitionCompactionKey() {
            return null;
        }

        @Override
        public void setPartitionCompactionKey(final String partitionCompactionKey) {

        }

        @Override
        public String getEventType() {
            return null;
        }

        @Override
        public void setEventType(final String eventType) {

        }
    };

}
