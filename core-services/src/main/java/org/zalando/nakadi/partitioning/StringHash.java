package org.zalando.nakadi.partitioning;

import org.springframework.stereotype.Component;

@Component
public class StringHash {

    public int hashCode(final String string) {
        int hash = 0;
        if (!string.isEmpty()) {
            for (int i = 0; i < string.length(); i++) {
                hash = 31 * hash + string.charAt(i);
            }
        }
        return hash;
    }

}
