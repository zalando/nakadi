package org.zalando.nakadi.domain;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.utils.TestUtils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class NakadiCursorTest {

    @Test
    public void whenValidateCommitCursorsThenOk() throws InvalidCursorException {
        NakadiCursor.of(TestUtils.buildTimelineWithTopic("tmp"), "0", "23").checkStorageAvailability();
    }

    @Test
    public void whenValidateInvalidCommitCursorsThenException() {
        ImmutableMap.of(
                NakadiCursor.of(TestUtils.buildTimelineWithTopic("tmp"), "345", "1"), CursorError.PARTITION_NOT_FOUND,
                NakadiCursor.of(TestUtils.buildTimelineWithTopic("tmp"), "0", "abc"), CursorError.INVALID_FORMAT)
                .entrySet()
                .forEach(testCase -> {
                    try {
                        testCase.getKey().checkStorageAvailability();
                    } catch (final InvalidCursorException e) {
                        assertThat(e.getError(), equalTo(testCase.getValue()));
                    }
                });
    }

}
