package org.zalando.nakadi.service.subscription;

import java.io.IOException;

public interface SubscriptionOutput {
    void onInitialized(String sessionId) throws IOException;

    void onException(Exception ex);

    void streamData(byte[] data) throws IOException;
}
