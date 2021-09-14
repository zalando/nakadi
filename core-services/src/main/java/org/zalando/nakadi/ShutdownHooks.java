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

    private final Set<Runnable> hooks = new HashSet<>();
    private static final Logger LOG = LoggerFactory.getLogger(ShutdownHooks.class);

    @PreDestroy
    private void onNodeShutdown() {
        LOG.info("Processing shutdown hooks");
        boolean haveHooks = true;
        while (haveHooks) {
            final Runnable hook;
            synchronized (hooks) {
                hook = hooks.isEmpty() ? null : hooks.iterator().next();
                hooks.remove(hook);
                haveHooks = !hooks.isEmpty();
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
        synchronized (hooks) {
            hooks.add(runnable);
        }
        return () -> removeHook(runnable);
    }

    private void removeHook(final Runnable runnable) {
        synchronized (hooks) {
            hooks.remove(runnable);
        }
    }

}
