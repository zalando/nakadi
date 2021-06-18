package org.zalando.nakadi.service.job;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

public class DummyJobWrapper {

    public static ExclusiveJobWrapper create() {
        final ExclusiveJobWrapper jobWrapper = mock(ExclusiveJobWrapper.class);
        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(jobWrapper).runJobLocked(any());
        return jobWrapper;
    }
}
