package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.service.subscription.state.State;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.Assert;
import org.junit.Test;

public class StreamingContextTest {
    private static StreamingContext createTestContext(final Consumer<Exception> onException) {
        final SubscriptionOutput output = new SubscriptionOutput() {
            @Override
            public void onInitialized() throws IOException {
            }

            @Override
            public void onException(final Exception ex) {
                if (null != onException) {
                    onException.accept(ex);
                }
            }

            @Override
            public void streamData(final byte[] data) throws IOException {

            }
        };

        return new StreamingContext(output, null, null, null, null, null, null, 0);
    }

    @Test
    public void streamingContextShouldStopOnException() throws InterruptedException {
        final AtomicReference<Exception> caughtException = new AtomicReference<>(null);
        final RuntimeException killerException = new RuntimeException();

        final StreamingContext ctx = createTestContext(caughtException::set);

        final State killerState = new State(ctx) {
            @Override
            public void onEnter() {
                throw killerException;
            }
        };
        final Thread t = new Thread(() -> {
            try {
                ctx.streamInternal(killerState);
            } catch (final InterruptedException ignore) {
            }
        });
        t.start();
        t.join(1000);
        // Check that thread died
        Assert.assertFalse(t.isAlive());

        // Check that correct exception was caught by output
        Assert.assertSame(killerException, caughtException.get());
    }

    @Test
    public void stateBaseMethodsMustBeCalledOnSwitching() throws InterruptedException {
        final StreamingContext ctx = createTestContext(null);
        final boolean[] onEnterCalls = new boolean[]{false, false};
        final boolean[] onExitCalls = new boolean[]{false, false};
        final State state1 = new State(ctx) {
            @Override
            public void onEnter() {
                onEnterCalls[0] = true;
                throw new RuntimeException(); // trigger stop.
            }

            @Override
            public void onExit() {
                onExitCalls[0] = true;
            }
        };
        final State state2 = new State(ctx) {
            @Override
            public void onEnter() {
                onEnterCalls[1] = true;
                context.switchState(state1);
            }

            @Override
            public void onExit() {
                onExitCalls[1] = true;
            }
        };

        ctx.streamInternal(state2);
        Assert.assertArrayEquals(new boolean[]{true, true}, onEnterCalls);
        // Check that onExit called even if onEnter throws exception.
        Assert.assertArrayEquals(new boolean[]{true, true}, onExitCalls);
    }
}