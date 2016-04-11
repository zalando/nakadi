package de.zalando.aruha.nakadi.repository.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.config.JsonConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;
import org.junit.Assert;
import org.junit.Test;

public class KafkaPartitionsCalculatorTest {

    private static final String TEST_DATA = "[" +
            "  {" +
            "    \"name\": \"t2.large\"," +
            "    \"stats\": [" +
            "      {" +
            "        \"message_size\": 10000," +
            "        \"speed\": [35.34, 55.52, 57.14, 76.96, 89.56, 97.54, 94.73, 96.69]" +
            "      }, {" +
            "        \"message_size\": 1000," +
            "        \"speed\": [48.12, 74.83, 92.89, 84.69, 91.19, 94.22, 88.34, 86.35]" +
            "      }, {" +
            "        \"message_size\": 100," +
            "        \"speed\": [25.27, 24.69, 25.48, 25.59, 24.95, 25.12, 25.17, 25.93]" +
            "      }" +
            "    ]" +
            "  }, {" +
            "    \"name\": \"c4.xlarge\"," +
            "    \"stats\": [" +
            "      {" +
            "        \"message_size\": 10000," +
            "        \"speed\": [41.83, 65.24, 112.33, 147.01, 142.45, 144.64, 146.53, 145.77]" +
            "      }, {" +
            "        \"message_size\": 1000," +
            "        \"speed\": [53.88, 116.18, 118.36, 89.95, 94.78, 88.82, 97.76, 89.89]" +
            "      }, {" +
            "        \"message_size\": 100," +
            "        \"speed\": [53.81, 54.04, 71.10, 63.35, 64.48, 69.21, 59.73, 70.29]" +
            "      }" +
            "    ]" +
            "  }" +
            "]";

    private static final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();

    public static InputStream getTestStream() {
        return new ByteArrayInputStream(TEST_DATA.getBytes());
    }

    @Test
    public void testLoadNullForUnknownName() throws IOException {
        try {
            KafkaPartitionsCalculator.load(objectMapper, "null", getTestStream());
            Assert.fail("Unknown calculator must throw IllegalArgumentException only");
        } catch (IllegalArgumentException ignored) {
        } catch (RuntimeException ex) {
            Assert.fail("Unknown calculator must throw IllegalArgumentException only, but found: " + ex.getClass().getName());
        }
    }

    @Test
    public void testLoadCorrectForCorrectName() throws IOException {
        for (String name : new String[]{"t2.large", "c4.xlarge"}) {
            final KafkaPartitionsCalculator calculatorMap = KafkaPartitionsCalculator.load(objectMapper, name, getTestStream());
            assertThat(calculatorMap, notNullValue());
        }
    }

    @Test
    public void ensureCorrectValuesReturnedForSimpleCase() throws IOException {
        final KafkaPartitionsCalculator calculator = KafkaPartitionsCalculator.load(objectMapper, "t2.large", getTestStream());
        // 48.12, 74.83, 92.89, 84.69, 91.19, 94.22, 88.34, 86.35
        assertThat(calculator.getBestPartitionsCount(1000, 48.12f), equalTo(1));
        assertThat(calculator.getBestPartitionsCount(1000, 48.13f), equalTo(2));
        assertThat(calculator.getBestPartitionsCount(1000, 74.82f), equalTo(2));
        assertThat(calculator.getBestPartitionsCount(1000, 74.84f), equalTo(3));
        assertThat(calculator.getBestPartitionsCount(1000, 92.88f), equalTo(3));
        assertThat(calculator.getBestPartitionsCount(1000, 92.90f), equalTo(6));
        // ensure that we will try our best to perform task.
        assertThat(calculator.getBestPartitionsCount(1000, 100.f), equalTo(6));
    }

    @Test
    public void ensureCorrectValuesReturnedForCentralCase() throws IOException {
        final KafkaPartitionsCalculator calculator = KafkaPartitionsCalculator.load(objectMapper, "t2.large", getTestStream());
        final int testCaseCount = 100;
        for (int i = 0; i < testCaseCount; ++i) {
            float mbsPerSecond = (100.f * i) / testCaseCount;
            final int countLower = calculator.getBestPartitionsCount(100, mbsPerSecond);
            final int countUpper = calculator.getBestPartitionsCount(1000, mbsPerSecond);
            final int countBetween = calculator.getBestPartitionsCount(550, mbsPerSecond);
            if (countLower > countUpper) {
                assertThat(countBetween, lessThanOrEqualTo(countLower));
                assertThat(countBetween, greaterThanOrEqualTo(countUpper));
            } else {
                assertThat(countBetween, lessThanOrEqualTo(countUpper));
                assertThat(countBetween, greaterThanOrEqualTo(countLower));
            }
        }
    }

    @Test
    public void testSmallSizes() throws IOException {
        final KafkaPartitionsCalculator calculator = KafkaPartitionsCalculator.load(objectMapper, "t2.large", getTestStream());
        // 25.27, 24.69, 25.48, 25.59, 24.95, 25.12, 25.17, 25.93
        assertThat(calculator.getBestPartitionsCount(0, 22.f), equalTo(1));
        assertThat(calculator.getBestPartitionsCount(0, 25.27f), equalTo(1));
        assertThat(calculator.getBestPartitionsCount(0, 25.28f), equalTo(3));
        assertThat(calculator.getBestPartitionsCount(0, 26.f), equalTo(8));
    }

    @Test
    public void testLargeSizes() throws IOException {
        final KafkaPartitionsCalculator calculator = KafkaPartitionsCalculator.load(objectMapper, "t2.large", getTestStream());
        // 35.34, 55.52, 57.14, 76.96, 89.56, 97.54, 94.73, 96.69
        assertThat(calculator.getBestPartitionsCount(200000, 22.f), equalTo(1));
        assertThat(calculator.getBestPartitionsCount(200000, 35.36f), equalTo(2));
        assertThat(calculator.getBestPartitionsCount(200000, 57.f), equalTo(3));
        assertThat(calculator.getBestPartitionsCount(200000, 96.f), equalTo(6));
        assertThat(calculator.getBestPartitionsCount(200000, 100.f), equalTo(6));
    }
}