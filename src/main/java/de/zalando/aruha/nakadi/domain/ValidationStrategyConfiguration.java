package de.zalando.aruha.nakadi.domain;

import org.json.JSONObject;

public class ValidationStrategyConfiguration {
private String strategyName;
private JSONObject additionalConfiguration;
public String getStrategyName() {
	return strategyName;
}
public void setStrategyName(final String strategyName) {
	this.strategyName = strategyName;
}
public JSONObject getAdditionalConfiguration() {
	return additionalConfiguration;
}
public void setAdditionalConfiguration(final JSONObject additionalConfiguration) {
	this.additionalConfiguration = additionalConfiguration;
}
}
