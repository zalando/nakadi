package org.zalando.nakadi.config;


import org.apache.log4j.NDC;
import org.slf4j.MDC;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.core.task.TaskDecorator;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ConcurrentTaskExecutor;

import java.util.concurrent.Executors;


@Configuration
@EnableScheduling
public class NakadiConfig {

    @Bean
    public AsyncTaskExecutor asyncTaskExecutor() {
        final ConcurrentTaskExecutor taskExecutor = new ConcurrentTaskExecutor(Executors.newCachedThreadPool());
        taskExecutor.setTaskDecorator(new TaskDecorator() {
            @Override
            public Runnable decorate(final Runnable runnable) {
                return new Runnable() {
                    @Override
                    public void run() {
                        NDC.clear();
                        MDC.clear();

                        runnable.run();
                    }
                };
            }
        });
        return taskExecutor;
    }
}
