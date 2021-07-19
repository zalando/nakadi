package org.zalando.nakadi;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;

import java.io.Closeable;
import java.util.HashSet;
import java.util.Set;

@Component
public class ShutdownHooks {

    private final Set<Runnable> HOOKS = new HashSet<>();
    private static final Logger LOG = LoggerFactory.getLogger(ShutdownHooks.class);

    @PreDestroy
    private void onNodeShutdown() {
        LOG.info("Processing shutdown hooks");
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
        LOG.info("Finished processing shutdown hooks");
    }

    public Closeable addHook(final Runnable runnable) {
        synchronized (HOOKS) {
            HOOKS.add(runnable);
        }
        return () -> removeHook(runnable);
    }

    private void removeHook(final Runnable runnable) {
        synchronized (HOOKS) {
            HOOKS.remove(runnable);
        }
    }

}
