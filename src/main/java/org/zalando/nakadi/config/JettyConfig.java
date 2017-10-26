package org.zalando.nakadi.config;

import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.embedded.jetty.JettyEmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.jetty.JettyServerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ArrayBlockingQueue;

@Configuration
public class JettyConfig {
    @Bean
    public JettyEmbeddedServletContainerFactory jettyEmbeddedServletContainerFactory(
            @Value("${server.port:8080}") final String port,
            @Value("${jetty.threadPool.maxThreads:200}") final String maxThreads,
            @Value("${jetty.threadPool.minThreads:10}") final String minThreads,
            @Value("${jetty.threadPool.idleTimeout:60000}") final String idleTimeout,
            @Value("${jetty.threadPool.queueSize:3000}") final String queueSize) {
        final JettyEmbeddedServletContainerFactory factory =
                new JettyEmbeddedServletContainerFactory(Integer.valueOf(port));

        final QueuedThreadPool queuedThreadPool = new QueuedThreadPool(Integer.valueOf(maxThreads),
                Integer.valueOf(minThreads),
                Integer.valueOf(idleTimeout),
                new ArrayBlockingQueue<>(Integer.valueOf(queueSize)));
        factory.setThreadPool(queuedThreadPool);

        factory.addServerCustomizers((JettyServerCustomizer) server -> {
            final GzipHandler gzipHandler = new GzipHandler();
            gzipHandler.setHandler(server.getHandler());
            gzipHandler.setSyncFlush(true);
            server.setHandler(gzipHandler);
        });
        return factory;
    }
}
