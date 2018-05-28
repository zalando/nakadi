package org.zalando.nakadi.config;

import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.embedded.jetty.JettyEmbeddedServletContainerFactory;
import org.springframework.boot.context.embedded.jetty.JettyServerCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JettyConfig {
    @Bean
    public JettyEmbeddedServletContainerFactory jettyEmbeddedServletContainerFactory(
            @Value("${server.port:8080}") final String port,
            @Value("${jetty.threadPool.maxThreads:200}") final String maxThreads,
            @Value("${jetty.threadPool.minThreads:8}") final String minThreads,
            @Value("${jetty.threadPool.idleTimeout:60000}") final String idleTimeout) {
        final JettyEmbeddedServletContainerFactory factory =
                new JettyEmbeddedServletContainerFactory(Integer.valueOf(port));
        factory.addServerCustomizers((JettyServerCustomizer) server -> {
            final QueuedThreadPool threadPool = server.getBean(QueuedThreadPool.class);
            threadPool.setMaxThreads(Integer.valueOf(maxThreads));
            threadPool.setMinThreads(Integer.valueOf(minThreads));
            threadPool.setIdleTimeout(Integer.valueOf(idleTimeout));

            final GzipHandler gzipHandler = new GzipHandler();
            gzipHandler.addIncludedMethods(HttpMethod.POST.asString());
            gzipHandler.setHandler(server.getHandler());
            gzipHandler.setSyncFlush(true);
            server.setHandler(gzipHandler);
        });
        return factory;
    }
}
