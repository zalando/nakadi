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

/**
 * Class wraps Hystrix API to count success/failures without using Hystrix command. In general we just use Hystrix to
 * collect metrics and check for opened circuit.
 */
public class HystrixKafkaCircuitBreaker {

    private static final HystrixCommandGroupKey HYSTRIX_CMD_GROUP_KEY = HystrixCommandGroupKey.Factory.asKey("kafka");

    private final HystrixCommandKey commandKey;
    private final HystrixCommandProperties commandProperties;
    private final HystrixThreadPoolKey threadPoolKey;
    private final HystrixCommandMetrics hystrixCommandMetrics;
    private final HystrixCircuitBreaker circuitBreaker;
    private final AtomicInteger concurrentExecutionCount;

    public HystrixKafkaCircuitBreaker(final String brokerId) {
        commandKey = HystrixCommandKey.Factory.asKey(brokerId);
        commandProperties = HystrixPropertiesFactory.getCommandProperties(commandKey, null);
        threadPoolKey = HystrixThreadPoolKey.Factory.asKey(brokerId);
        hystrixCommandMetrics = HystrixCommandMetrics.getInstance(
                commandKey,
                HYSTRIX_CMD_GROUP_KEY,
                threadPoolKey,
                commandProperties);
        circuitBreaker = HystrixCircuitBreaker.Factory.getInstance(
                commandKey,
                HYSTRIX_CMD_GROUP_KEY,
                commandProperties,
                hystrixCommandMetrics);
        concurrentExecutionCount = new AtomicInteger();
    }
    
    public boolean attemptExecution() {
        return circuitBreaker.attemptExecution();
    }

    public void markStart() {
        final int currentCount = concurrentExecutionCount.incrementAndGet();
        HystrixThreadEventStream.getInstance().commandExecutionStarted(commandKey, threadPoolKey,
                HystrixCommandProperties.ExecutionIsolationStrategy.SEMAPHORE, currentCount);
    }

    public void markSuccessfully() {
        concurrentExecutionCount.decrementAndGet();
        HystrixThreadEventStream.getInstance()
                .executionDone(ExecutionResult.from(HystrixEventType.SUCCESS), commandKey, threadPoolKey);
        circuitBreaker.markSuccess();
    }

    public void markFailure() {
        concurrentExecutionCount.decrementAndGet();
        HystrixThreadEventStream.getInstance()
                .executionDone(ExecutionResult.from(HystrixEventType.FAILURE), commandKey, threadPoolKey);
        circuitBreaker.markNonSuccess();
    }

    public String getMetrics() {
        return hystrixCommandMetrics.getHealthCounts().toString();
    }

}
