package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.service.subscription.model.Session;
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
            public void onInitialized(final String ignore) throws IOException {
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

        return new StreamingContext(output, null, Session.generate(1), null, null, null, null, 0, "stream");
    }

    @Test
    public void streamingContextShouldStopOnException() throws InterruptedException {
        final AtomicReference<Exception> caughtException = new AtomicReference<>(null);
        final RuntimeException killerException = new RuntimeException();

        final StreamingContext ctx = createTestContext(caughtException::set);

        final State killerState = new State() {
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
        final boolean[] contextsSet = new boolean[]{false, false};
        final State state1 = new State() {
            @Override
            public void setContext(final StreamingContext context, final String tmp) {
                super.setContext(context, tmp);
                contextsSet[0] = null != context;
            }

            @Override
            public void onEnter() {
                Assert.assertTrue(contextsSet[0]);
                onEnterCalls[0] = true;
                throw new RuntimeException(); // trigger stop.
            }

            @Override
            public void onExit() {
                onExitCalls[0] = true;
            }
        };
        final State state2 = new State() {
            @Override
            public void setContext(final StreamingContext context, final String tmp) {
                super.setContext(context, tmp);
                contextsSet[1] = true;
            }

            @Override
            public void onEnter() {
                Assert.assertTrue(contextsSet[1]);
                onEnterCalls[1] = true;
                switchState(state1);
            }

            @Override
            public void onExit() {
                onExitCalls[1] = true;
            }
        };

        ctx.streamInternal(state2);
        Assert.assertArrayEquals(new boolean[]{true, true}, contextsSet);
        Assert.assertArrayEquals(new boolean[]{true, true}, onEnterCalls);
        // Check that onExit called even if onEnter throws exception.
        Assert.assertArrayEquals(new boolean[]{true, true}, onExitCalls);
    }
}