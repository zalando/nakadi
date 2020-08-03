package org.zalando.nakadi.config;

import org.eclipse.jetty.http.HttpMethod;
import org.eclipse.jetty.server.handler.gzip.GzipHandler;
import org.eclipse.jetty.util.thread.QueuedThreadPool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class JettyConfig {
    @Bean
    public JettyServletWebServerFactory jettyServletWebServerFactory(
            @Value("${server.port:8080}") final String port,
            @Value("${jetty.threadPool.maxThreads:200}") final String maxThreads,
            @Value("${jetty.threadPool.minThreads:8}") final String minThreads,
            @Value("${jetty.threadPool.idleTimeout:60000}") final String idleTimeout) {
        final JettyServletWebServerFactory factory =
                new JettyServletWebServerFactory(Integer.parseInt(port));
        factory.addServerCustomizers(server -> {
            final QueuedThreadPool threadPool = server.getBean(QueuedThreadPool.class);
            threadPool.setMaxThreads(Integer.parseInt(maxThreads));
            threadPool.setMinThreads(Integer.parseInt(minThreads));
            threadPool.setIdleTimeout(Integer.parseInt(idleTimeout));

            final GzipHandler gzipHandler = new GzipHandler();
            gzipHandler.addIncludedMethods(HttpMethod.POST.asString());
            gzipHandler.setHandler(server.getHandler());
            gzipHandler.setSyncFlush(true);
            server.setHandler(gzipHandler);
        });
        return factory;
    }
}
