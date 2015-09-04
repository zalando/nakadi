package de.zalando.bazaar.lab;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class OperationsTest {

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
