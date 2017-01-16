package org.zalando.nakadi.repository.db;

import java.io.IOException;
import java.util.Collections;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.validation.EventBodyMustRespectSchema;
import org.zalando.nakadi.validation.EventMetadataValidationStrategy;
import org.zalando.nakadi.validation.JsonSchemaEnrichment;
import org.zalando.nakadi.validation.ValidationStrategy;

public class EventTypeCacheTest {
    @BeforeClass
    public static void initValidation() {
        ValidationStrategy.register(EventBodyMustRespectSchema.NAME, new EventBodyMustRespectSchema(
                new JsonSchemaEnrichment()
        ));
        ValidationStrategy.register(EventMetadataValidationStrategy.NAME, new EventMetadataValidationStrategy());
    }

    @Test
    public void testEventTypesPreloaded() throws InternalNakadiException, NoSuchEventTypeException, IOException {
        final EventTypeRepository etRepo = Mockito.mock(EventTypeRepository.class);
        final ZooKeeperHolder zkHolder = Mockito.mock(ZooKeeperHolder.class);

        final EventType testEventType = TestUtils.buildDefaultEventType();
        Mockito.when(etRepo.list()).thenReturn(Collections.singletonList(testEventType));

        final EventTypeCache eventTypeCache = new EventTypeCache(etRepo, zkHolder, null) {
            @Override
            public void created(final String name) throws Exception {
                // ignore this call, because mocking is too complex
            }
        };

        Assert.assertSame(testEventType, eventTypeCache.getEventType(testEventType.getName()));

        Mockito.verify(etRepo, Mockito.times(0)).findByName(Mockito.any());
        Mockito.verify(etRepo, Mockito.times(1)).list();
    }
}
