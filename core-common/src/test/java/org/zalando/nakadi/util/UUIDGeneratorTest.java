package org.zalando.nakadi.util;

import org.junit.Assert;
import org.junit.Test;

public class UUIDGeneratorTest {

    @Test
    public void shouldFailOnEmptyString() throws Exception {
        final UUIDGenerator uuidGenerator = new UUIDGenerator();
        Assert.assertFalse(uuidGenerator.isUUID(""));
    }

    @Test
    public void shouldFailWhenStringIsNotUUID() throws Exception {
        final UUIDGenerator uuidGenerator = new UUIDGenerator();
        Assert.assertFalse(uuidGenerator.isUUID("/"));
        Assert.assertFalse(uuidGenerator.isUUID("a"));
        Assert.assertFalse(uuidGenerator.isUUID("7"));
        // correct e1ee5f19-cd93-408e-abe8-374440d3be45 (no last digit)
        Assert.assertFalse(uuidGenerator.isUUID("e1ee5f19-cd93-408e-abe8-374440d3be4"));
    }

    @Test
    public void shouldSuccsessOnUUID() throws Exception {
        final UUIDGenerator uuidGenerator = new UUIDGenerator();
        Assert.assertTrue(uuidGenerator.isUUID("e1ee5f19-cd93-408e-abe8-374440d3be45"));
    }
}
