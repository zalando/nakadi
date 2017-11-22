package org.zalando.nakadi.service.kpi;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.service.kpi.publisher.NakadiKpiPublisher;

import java.util.function.Supplier;

@Service
public class NakadiAccessLogPublisher {

    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final String nakadiAccessLogEventType;

    @Autowired
    public NakadiAccessLogPublisher(
            final NakadiKpiPublisher nakadiKpiPublisher,
            @Value("${nakadi.kpi.event-types.nakadi.access.log:nakadi.access.log}") final String etName) {
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.nakadiAccessLogEventType = etName;
    }

    public void publishAccessLog(final Supplier<JSONObject> data) {
        nakadiKpiPublisher.publish(nakadiAccessLogEventType, data);
    }

}
