package de.zalando.aruha.nakadi.domain;

public class Topic {
  private String name;

  public Topic(final String name) {
    setName(name);
  }

  public String getName() {
    return name;
  }

  public void setName(final String name) {
    this.name = name;
  }

  @Override
  public String toString() {
    return "Topic [name=" + name + "]";
  }
}
