package de.zalando.aruha.nakadi.domain;

import java.util.List;

import com.google.common.collect.Lists;

public class EventType {

  private String name;
  private final List<ValidationStrategyConfiguration> validationStrategies = Lists.newArrayList();
  private EventTypeSchema eventTypeSchema;

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  public List<ValidationStrategyConfiguration> getValidationStrategies() {
    return validationStrategies;
  }

  public EventTypeSchema getEventTypeSchema() {
    return eventTypeSchema;
  }

  public void setEventTypeSchema(final EventTypeSchema eventTypeSchema) {
    this.eventTypeSchema = eventTypeSchema;
  }
}
