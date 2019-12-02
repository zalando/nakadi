package org.zalando.nakadi.service.subscription;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.state.CleanupState;
import org.zalando.nakadi.service.subscription.state.DummyState;
import org.zalando.nakadi.service.subscription.state.State;
import org.zalando.nakadi.util.ThreadUtils;

import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

public class StreamingContextTest {
    private static StreamingContext createTestContext(final Consumer<Exception> onException) {
        final SubscriptionOutput output = new SubscriptionOutput() {
            @Override
            public void onInitialized(final String ignore) {
            }

            @Override
            public void onException(final Exception ex) {
                if (null != onException) {
                    onException.accept(ex);
                }
            }

            @Override
            public OutputStream getOutputStream() {
                return null;
            }
        };
        return new StreamingContext.Builder()
                .setOut(output)
                .setParameters(null)
                .setSession(Session.generate(1, ImmutableList.of()))
                .setSubscription(new Subscription())
                .setTimer(null)
                .setZkClient(null)
                .setRebalancer(null)
                .setKafkaPollTimeout(0)
                .setConnectionReady(new AtomicBoolean(true))
                .setCursorTokenService(null)
                .setObjectMapper(null)
                .setBlacklistService(null)
                .build();
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
            public void setContext(final StreamingContext context) {
                super.setContext(context);
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
            public void setContext(final StreamingContext context) {
                super.setContext(context);
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

    @Test
    @Ignore
    public void testOnNodeShutdown() throws Exception {
        final StreamingContext ctxSpy = Mockito.spy(createTestContext(null));
        final Thread t = new Thread(() -> {
            try {
                ctxSpy.streamInternal(new State() {
                    @Override
                    public void onEnter() {
                    }
                });
            } catch (final InterruptedException ignore) {
            }
        });
        t.start();
        t.join(1000);

        new Thread(() -> ctxSpy.onNodeShutdown()).start();
        ThreadUtils.sleep(2000);

        Mockito.verify(ctxSpy).switchState(Mockito.isA(CleanupState.class));
        Mockito.verify(ctxSpy).unregisterSession();
        Mockito.verify(ctxSpy).switchState(Mockito.isA(DummyState.class));
    }
}
