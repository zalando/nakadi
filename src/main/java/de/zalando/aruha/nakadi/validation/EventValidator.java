package de.zalando.aruha.nakadi.validation;

import org.json.JSONObject;

public interface EventValidator {
  boolean accepts(final JSONObject event);
}
