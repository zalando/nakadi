package org.zalando.nakadi.service.subscription;

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.generated.avro.Envelope;
import org.zalando.nakadi.generated.avro.Metadata;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.repository.kafka.KafkaRecordDeserializer;
import org.zalando.nakadi.service.EventStreamChecks;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.service.SchemaProviderService;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.state.CleanupState;
import org.zalando.nakadi.service.subscription.state.DummyState;
import org.zalando.nakadi.service.subscription.state.State;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.util.ThreadUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamingContextTest {
    private static StreamingContext createTestContext(final Consumer<Exception> onException) throws IOException {
        final SubscriptionOutput output = new SubscriptionOutput() {
            @Override
            public void onInitialized(final String ignore) {
            }

            @Override
            public void onException(final Exception ex) {
                if (null != onException) {
                    onException.accept(ex);
                }
            }

            @Override
            public OutputStream getOutputStream() {
                return null;
            }
        };

        // Mocks
        final ZkSubscriptionClient zkClient = mock(ZkSubscriptionClient.class);
        doNothing().when(zkClient).close();

        return new StreamingContext.Builder()
                .setOut(output)
                .setParameters(null)
                .setSession(Session.generate(1, ImmutableList.of()))
                .setSubscription(new Subscription())
                .setTimer(null)
                .setZkClient(zkClient)
                .setRebalancer(null)
                .setKafkaPollTimeout(0)
                .setCursorTokenService(null)
                .setObjectMapper(null)
                .setEventStreamChecks(null)
                .build();
    }

    @Test
    public void streamingContextShouldStopOnException() throws InterruptedException, IOException {
        final AtomicReference<Exception> caughtException = new AtomicReference<>(null);
        final RuntimeException killerException = new RuntimeException();

        final StreamingContext ctx = createTestContext(caughtException::set);

        final State killerState = new State() {
            @Override
            public void onEnter() {
                throw killerException;
            }
        };
        final Thread t = new Thread(() -> {
            try {
                ctx.streamInternal(killerState);
            } catch (final InterruptedException ignore) {
            }
        });
        t.start();
        t.join(1000);
        // Check that thread died
        Assert.assertFalse(t.isAlive());

        // Check that correct exception was caught by output
        Assert.assertSame(killerException, caughtException.get());
    }

    @Test
    public void stateBaseMethodsMustBeCalledOnSwitching() throws InterruptedException, IOException {
        final StreamingContext ctx = createTestContext(null);
        final boolean[] onEnterCalls = new boolean[]{false, false};
        final boolean[] onExitCalls = new boolean[]{false, false};
        final boolean[] contextsSet = new boolean[]{false, false};
        final State state1 = new State() {
            @Override
            public void setContext(final StreamingContext context) {
                super.setContext(context);
                contextsSet[0] = null != context;
            }

            @Override
            public void onEnter() {
                Assert.assertTrue(contextsSet[0]);
                onEnterCalls[0] = true;
                throw new RuntimeException(); // trigger stop.
            }

            @Override
            public void onExit() {
                onExitCalls[0] = true;
            }
        };
        final State state2 = new State() {
            @Override
            public void setContext(final StreamingContext context) {
                super.setContext(context);
                contextsSet[1] = true;
            }

            @Override
            public void onEnter() {
                Assert.assertTrue(contextsSet[1]);
                onEnterCalls[1] = true;
                switchState(state1);
            }

            @Override
            public void onExit() {
                onExitCalls[1] = true;
            }
        };

        ctx.streamInternal(state2);
        Assert.assertArrayEquals(new boolean[]{true, true}, contextsSet);
        Assert.assertArrayEquals(new boolean[]{true, true}, onEnterCalls);
        // Check that onExit called even if onEnter throws exception.
        Assert.assertArrayEquals(new boolean[]{true, true}, onExitCalls);
    }

    @Test
    @Ignore
    public void testOnNodeShutdown() throws Exception {
        final StreamingContext ctxSpy = Mockito.spy(createTestContext(null));
        final Thread t = new Thread(() -> {
            try {
                ctxSpy.streamInternal(new State() {
                    @Override
                    public void onEnter() {
                    }
                });
            } catch (final InterruptedException ignore) {
            }
        });
        t.start();
        t.join(1000);

        new Thread(() -> ctxSpy.terminateStream()).start();
        ThreadUtils.sleep(2000);

        Mockito.verify(ctxSpy).switchState(Mockito.isA(CleanupState.class));
        Mockito.verify(ctxSpy).unregisterSession();
        Mockito.verify(ctxSpy).switchState(Mockito.isA(DummyState.class));
    }

    @Test
    public void testSessionAlwaysCleanedManyTimesOk() {

        final ZkSubscriptionClient zkMock = mock(ZkSubscriptionClient.class);
        when(zkMock.isActiveSession(any())).thenReturn(true);

        final StreamingContext context = new StreamingContext.Builder()
                .setSession(Session.generate(1, ImmutableList.of()))
                .setSubscription(new Subscription())
                .setZkClient(zkMock)
                .setKafkaPollTimeout(0)
                .build();

        doThrow(new NakadiRuntimeException(new Exception("Failed!"))).when(zkMock).registerSession(any());
        assertThrows(NakadiRuntimeException.class, () -> context.registerSession());

        // CleanupState calls context.unregisterSession() in finally block
        context.unregisterSession();

        // It can also be called many times and should not result in exceptions
        context.unregisterSession();

        Mockito.verify(zkMock, Mockito.times(1)).unregisterSession(any());
    }


    @Test
    public void testIncorrectEventTypeDestination() throws IOException {
        final var incorrectEventTypeName = "incorrect_event_type";
        final var supportedEventTypeName = "correct_event_type";
        final var subscription = new Subscription();
        subscription.setEventTypes(Set.of(supportedEventTypeName));
        final var et = new EventType();
        et.setCategory(EventCategory.BUSINESS);

        final var eventStreamCheck = mock(EventStreamChecks.class);
        final var schemaProvider = mock(SchemaProviderService.class);
        final var featureToggleService = mock(FeatureToggleService.class);
        final var etCache = mock(EventTypeCache.class);
        final var deserializer = new KafkaRecordDeserializer(
                new NakadiRecordMapper(mock(LocalSchemaRegistry.class)), schemaProvider);

        final Schema schema = Schema.createRecord("Foo", null, null, false);
        schema.setFields(List.of(new Schema.Field("name", Schema.create(Schema.Type.STRING), null, null)));

        when(schemaProvider.getAvroSchema(incorrectEventTypeName, "1.0.0")).
                thenReturn(schema);
        when(eventStreamCheck.isConsumptionBlocked(any())).
                thenReturn(false);
        when(etCache.getEventType(eq(supportedEventTypeName))).thenReturn(et);
        when(featureToggleService.isFeatureEnabled(Feature.SKIP_MISPLACED_EVENTS)).thenReturn(true);

        final StreamingContext context = new StreamingContext.Builder()
                .setSubscription(subscription)
                .setEventStreamChecks(eventStreamCheck)
                .setKafkaRecordDeserializer(deserializer)
                .setEventTypeCache(etCache)
                .setKafkaPollTimeout(0)
                .setFeatureToggleService(featureToggleService)
                .build();

        final String noMetadataEvent = "{}";
        final String noEventTypeEvent = "{\"metadata\": {}}";
        final String incorrectEvent = String.
                format("{\"metadata\": {\"event_type\": \"%s\"}, \"foo\": \"bar\"}", incorrectEventTypeName);
        final String correctEvent = String.
                format("{\"metadata\": {\"event_type\": \"%s\"}, \"foo\": \"bar\"}", supportedEventTypeName);

        final var cursor = NakadiCursor.of(Timeline.createTimeline(
                supportedEventTypeName, 0, new Storage(null, Storage.Type.KAFKA), null, null),
                        null, null);
        final Predicate<byte[]> isConsumptionBlocked =
                bytes -> context.isConsumptionBlocked(
                        new ConsumedEvent(bytes, cursor, 0L, null, Collections.emptyMap()));

        Assert.assertEquals(true, isConsumptionBlocked.test(noMetadataEvent.getBytes()));
        Assert.assertEquals(true, isConsumptionBlocked.test(noEventTypeEvent.getBytes()));
        Assert.assertEquals(true, isConsumptionBlocked.test(incorrectEvent.getBytes()));
        Assert.assertEquals(false, isConsumptionBlocked.test(correctEvent.getBytes()));

        final var incorrectRecord = getAvroRecordBytes(incorrectEventTypeName, schema);
        final var correctRecord = getAvroRecordBytes(supportedEventTypeName, schema);

        Assert.assertEquals(true, isConsumptionBlocked.test(incorrectRecord));
        Assert.assertEquals(false, isConsumptionBlocked.test(correctRecord));
    }

    private static byte[] getAvroRecordBytes(final String eventTypeName,
                                             final Schema schema) throws IOException {
        final GenericRecord record = new GenericData.Record(schema);
        record.put("name", "foo");

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new GenericDatumWriter(schema).write(
                record,
                EncoderFactory.get().directBinaryEncoder(baos, null));

        final var event = Envelope.newBuilder()
                        .setMetadata(Metadata.newBuilder()
                                .setEventType(eventTypeName)
                                .setVersion("1.0.0")
                                .setOccurredAt(Instant.now())
                                .setEid(UUID.randomUUID().toString())
                                .build())
                        .setPayload(ByteBuffer.wrap(baos.toByteArray()))
                        .build();

        return Envelope.getEncoder().encode(event).array();
    }


}
