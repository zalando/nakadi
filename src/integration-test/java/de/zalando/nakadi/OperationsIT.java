package de.zalando.nakadi;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import de.zalando.aruha.nakadi.Operations;

public class OperationsIT {

  private Operations operations = new Operations();

  @Test
  public void addTwoNumbers() throws Exception {
    // given

    // that
    final int result = operations.add(1, 2);

    // when
    assertEquals(3, result);
  }
}
