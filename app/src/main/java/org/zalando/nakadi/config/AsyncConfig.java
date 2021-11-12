package org.zalando.nakadi.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
public class AsyncConfig {
    @Bean
    @Qualifier("mvcAsyncTaskExecutor")
    public AsyncTaskExecutor taskExecutor(
            @Value("${webmvc.async.threadPool.coreSize:8}") final String corePoolSize,
            @Value("${webmvc.async.threadPool.maxSize:200}") final String maxPoolSize) {

        final ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(Integer.valueOf(corePoolSize));
        executor.setMaxPoolSize(Integer.valueOf(maxPoolSize));
        return executor;
    }
}
