package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;

public class StringHashTest {

    private final StringHash stringHash = new StringHash();

    private static final Map<String, Integer> EXPECTED_HASHES = ImmutableMap.<String, Integer>builder()
            .put("", 0)
            .put("a", 97)
            .put("Hello", 69609650)
            .put("<><%?>#<)#%(<*J%H\n\n)R&#89hb678n(*njyih48p3u bn7\t843h 743h yh", -706818463)
            .put("vcnvbjfji  b bn    vhfufh47---=--++_)9 u48 4", 1046847700)
            .put("❂◥ ✙ ┒bn↿☺ r☺⃐▕   ➀ ❸☋⃑ ├⒦  ❏ ⍘➍⑩Ⓝ☃⍎∆ ┏ ✉ ℛ", -1030241478)
            .build();

    @Test
    public void testHashValueIsCorrectAndEqualsToStringHash() {
        EXPECTED_HASHES.forEach((testString, expectedHash) ->
                assertThat(
                        stringHash.hashCode(testString),
                        both(equalTo(expectedHash))
                                .and(equalTo(testString.hashCode()))));
    }

}
