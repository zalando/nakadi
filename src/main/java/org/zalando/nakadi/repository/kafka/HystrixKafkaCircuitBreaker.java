package org.zalando.nakadi.repository.kafka;

import com.netflix.hystrix.ExecutionResult;
import com.netflix.hystrix.HystrixCircuitBreaker;
import com.netflix.hystrix.HystrixCommandGroupKey;
import com.netflix.hystrix.HystrixCommandKey;
import com.netflix.hystrix.HystrixCommandMetrics;
import com.netflix.hystrix.HystrixCommandProperties;
import com.netflix.hystrix.HystrixEventType;
import com.netflix.hystrix.HystrixThreadPoolKey;
import com.netflix.hystrix.metric.HystrixThreadEventStream;
import com.netflix.hystrix.strategy.properties.HystrixPropertiesFactory;

import java.util.concurrent.atomic.AtomicInteger;

public class HystrixKafkaCircuitBreaker {

    private static final HystrixCommandGroupKey hystrixCommandGroupKey = HystrixCommandGroupKey.Factory.asKey("kafka");

    private final HystrixCommandKey commandKey;
    private final HystrixCommandProperties commandProperties;
    private final HystrixThreadPoolKey threadPoolKey;
    private final HystrixCommandMetrics hystrixCommandMetrics;
    private final HystrixCircuitBreaker circuitBreaker;
    private final AtomicInteger concurrentExecutionCount = new AtomicInteger();

    public HystrixKafkaCircuitBreaker(final String brokerId) {
        commandKey = HystrixCommandKey.Factory.asKey(brokerId);
        commandProperties = HystrixPropertiesFactory.getCommandProperties(commandKey, null);
        threadPoolKey = HystrixThreadPoolKey.Factory.asKey(brokerId);
        hystrixCommandMetrics = HystrixCommandMetrics.getInstance(
                commandKey,
                hystrixCommandGroupKey,
                threadPoolKey,
                commandProperties);
        circuitBreaker = HystrixCircuitBreaker.Factory.getInstance(
                commandKey,
                hystrixCommandGroupKey,
                commandProperties,
                hystrixCommandMetrics);
    }

    public boolean allowRequest() {
        return circuitBreaker.allowRequest();
    }

    public void markCommandStart() {
        int currentCount = concurrentExecutionCount.incrementAndGet();
        HystrixThreadEventStream.getInstance().commandExecutionStarted(commandKey, threadPoolKey,
                HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE, currentCount);
    }

    public void markCommandDoneSuccessfully() {
        concurrentExecutionCount.decrementAndGet();
        HystrixThreadEventStream.getInstance()
                .executionDone(ExecutionResult.from(HystrixEventType.SUCCESS), commandKey, threadPoolKey);
        circuitBreaker.markSuccess();
    }

    public void markCommandDoneFailure() {
        concurrentExecutionCount.decrementAndGet();
        HystrixThreadEventStream.getInstance()
                .executionDone(ExecutionResult.from(HystrixEventType.FAILURE), commandKey, threadPoolKey);
    }

    public String getMetrics() {
        return hystrixCommandMetrics.getHealthCounts().toString();
    }

}
