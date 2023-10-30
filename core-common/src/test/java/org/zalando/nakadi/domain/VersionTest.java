package org.zalando.nakadi.domain;

import org.junit.Test;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class VersionTest {
    @Test
    public void bumpShouldZeroSmallerVersionNumber() throws Exception {
        assertThat(new Version("1.1.0").bump(Version.Level.MAJOR), equalTo(new Version("2.0.0")));
        assertThat(new Version("1.1.1").bump(Version.Level.MINOR), equalTo(new Version("1.2.0")));
    }
}