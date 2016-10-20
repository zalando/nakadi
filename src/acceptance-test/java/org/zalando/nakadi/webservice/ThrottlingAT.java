package org.zalando.nakadi.webservice;

import org.junit.Test;
import org.zalando.nakadi.domain.EventType;

import static java.util.stream.IntStream.rangeClosed;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishEvent;

public class ThrottlingAT extends BaseAT {

    @Test(timeout = 30000)
    public void throttleAfter10Messages() throws Exception {
        final EventType eventType = createEventType();
        rangeClosed(0, 9)
                .forEach(x -> publishEvent(eventType.getName(), "{\"blah\":\"foo" + x + "\"}")
                        .then().statusCode(200));

        publishEvent(eventType.getName(), "{\"blah\":\"foo\"}")
                .then().statusCode(429);
    }

}
