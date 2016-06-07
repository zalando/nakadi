package de.zalando.aruha.nakadi.service.subscription;

import java.io.IOException;

public interface SubscriptionOutput {
    void onInitialized() throws IOException;

    void onException(Exception ex);

    void streamData(byte[] data) throws IOException;
}
