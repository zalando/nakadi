package org.zalando.nakadi.service.publishing.check;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiAvroMetadata;
import org.zalando.nakadi.domain.NakadiRecord;

import java.util.List;

public class CheckTest {

    @Test
    public void testAbortRecordsBeforeAndAfterFailedRecord() {
        final var nakadiMetadata = getTestMetadata();
        final NakadiRecord recordOne = new NakadiRecord().setMetadata(nakadiMetadata);
        final NakadiRecord failedRecord = new NakadiRecord().setMetadata(nakadiMetadata);
        final NakadiRecord recordThree = new NakadiRecord().setMetadata(nakadiMetadata);
        final List<NakadiRecord> records = Lists.newArrayList(
                recordOne, failedRecord, recordThree);

        final List<Check.RecordResult> results =
                check.processError(records, failedRecord, "test-error");
        Assert.assertEquals(Check.Status.ABORTED, results.get(0).getStatus());
        Assert.assertEquals(Check.Step.VALIDATION, results.get(0).getStep());
        Assert.assertEquals("", results.get(0).getError());

        Assert.assertEquals(Check.Status.FAILED, results.get(1).getStatus());
        Assert.assertEquals(Check.Step.VALIDATION, results.get(1).getStep());
        Assert.assertEquals("test-error", results.get(1).getError());

        Assert.assertEquals(Check.Status.ABORTED, results.get(2).getStatus());
        Assert.assertEquals(Check.Step.NONE, results.get(2).getStep());
        Assert.assertEquals("", results.get(2).getError());
    }

    private final Check check = new Check() {
        @Override
        public List<RecordResult> execute(final EventType eventType,
                                          final List<NakadiRecord> records) {
            return null;
        }

        @Override
        public Step getCurrentStep() {
            return Step.VALIDATION;
        }
    };

    private NakadiAvroMetadata getTestMetadata() {
        final NakadiAvroMetadata nakadiMetadata = new NakadiAvroMetadata((byte) 0, null);
        nakadiMetadata.setEid("12345");
        return nakadiMetadata;
    }
}
