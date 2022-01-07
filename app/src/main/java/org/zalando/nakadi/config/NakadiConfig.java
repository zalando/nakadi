package org.zalando.nakadi.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;

import java.util.concurrent.Executors;


@Configuration
@EnableScheduling
public class NakadiConfig {

    @Bean
    public AsyncTaskExecutor asyncTaskExecutor() {
        return new ConcurrentTaskExecutor(Executors.newCachedThreadPool());
    }
}
