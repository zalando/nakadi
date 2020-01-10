package org.zalando.nakadi.service.subscription;

import java.io.IOException;
import java.io.OutputStream;

public interface SubscriptionOutput {
    void onInitialized(String sessionId) throws IOException;

    void onException(Exception ex);

    OutputStream getOutputStream();
}
