package org.zalando.nakadi.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;

import java.util.concurrent.Executors;

@Configuration
public class AsyncConfig {
    @Bean
    @Qualifier("mvcAsyncTaskExecutor")
    public AsyncTaskExecutor taskExecutor() {
        return new ConcurrentTaskExecutor(Executors.newCachedThreadPool());
    }
}
