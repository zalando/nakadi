package org.zalando.nakadi.security;

import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class UsernameHasherTest {

    @Test
    public void testHash() {
        final UsernameHasher usernameHasher = new UsernameHasher("123");
        assertThat(
                usernameHasher.hash("abc"),
                equalTo("dd130a849d7b29e5541b05d2f7f86a4acd4f1ec598c1c9438783f56bc4f0ff80"));
    }
}
