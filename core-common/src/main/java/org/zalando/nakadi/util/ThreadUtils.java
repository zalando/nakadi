package org.zalando.nakadi.util;

public class ThreadUtils {

    public static void sleep(final long ms) throws InterruptedException {
        final long startAt = System.currentTimeMillis();
        final long finishAt = startAt + ms;
        long toSleep = ms;
        while (true) {
            Thread.sleep(toSleep);
            final long now = System.currentTimeMillis();
            if (now >= finishAt) {
                break;
            }
            toSleep = finishAt - now;
        }
    }
}
