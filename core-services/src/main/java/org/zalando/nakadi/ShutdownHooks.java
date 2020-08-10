package org.zalando.nakadi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;

public class ShutdownHooks {

    private static final Set<Runnable> HOOKS = new HashSet<>();
    private static final Logger LOG = LoggerFactory.getLogger(ShutdownHooks.class);

    static {
        Runtime.getRuntime().addShutdownHook(new Thread(ShutdownHooks::onNodeShutdown));
    }

    private static void onNodeShutdown() {
        System.out.println("Processing shutdown hooks");
        boolean haveHooks = true;
        while (haveHooks) {
            final Runnable hook;
            synchronized (HOOKS) {
                hook = HOOKS.isEmpty() ? null : HOOKS.iterator().next();
                HOOKS.remove(hook);
                haveHooks = !HOOKS.isEmpty();
            }
            if (null != hook) {
                try {
                    hook.run();
                } catch (final RuntimeException ex) {
                    LOG.warn("Failed to call on shutdown hook for {}", hook, ex);
                }
            }
        }
        System.out.println("Finished processing shutdown hooks");
    }

    public static Closeable addHook(final Runnable runnable) {
        synchronized (HOOKS) {
            HOOKS.add(runnable);
        }
        return () -> removeHook(runnable);
    }

    private static void removeHook(final Runnable runnable) {
        synchronized (HOOKS) {
            HOOKS.remove(runnable);
        }
    }

}
