package org.zalando.nakadi.domain;

import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class VersionTest {
    @Test
    public void bumpShouldZeroSmallerVersionNumber() throws Exception {
        assertThat(new JsonVersion("1.1.0").bump(JsonVersion.Level.MAJOR), equalTo(new JsonVersion("2.0.0")));
        assertThat(new JsonVersion("1.1.1").bump(JsonVersion.Level.MINOR), equalTo(new JsonVersion("1.2.0")));
    }
}