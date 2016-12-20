package org.zalando.nakadi.domain;

import org.json.JSONObject;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class ValidationStrategyConfiguration {
    private String strategyName;
    private JSONObject additionalConfiguration;
}
