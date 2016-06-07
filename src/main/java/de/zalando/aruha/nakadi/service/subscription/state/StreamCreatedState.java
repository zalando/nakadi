package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.service.subscription.StreamingContext;

/**
 * Fake streaming state is needed to avoid flags in streaming context.
 */
public class StreamCreatedState extends State {
    public StreamCreatedState(final StreamingContext context) {
        super(context);
    }

    @Override
    public void onEnter() {
    }
}
