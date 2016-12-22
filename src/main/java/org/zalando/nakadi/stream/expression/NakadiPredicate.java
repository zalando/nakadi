package org.zalando.nakadi.stream.expression;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.stream.Utils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Predicate;

/**
 * <pre>
 *
 * Expression: filter warehouse.price    <         10
 *              ^           ^            ^         ^
 *             type   firstParam      operator  secondParam
 *</pre>
 */
public final class NakadiPredicate {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiPredicate.class);
    private static final int WORDS_IN_PREDICATE = 4;
    private static final String FILTER = ExpressionType.FILTER.toString().toLowerCase();
    private static final Map<String, BiFunction<Integer, Integer, Boolean>> OPERATION_INT_TO_FUNC = ImmutableMap.of(
            ">", (v1, v2) -> v1 > v2,
            "<", (v1, v2) -> v1 < v2,
            "=", (v1, v2) -> v1 == v2
    );
    private static final Map<String, BiFunction<String, String, Boolean>> OPERATION_STR_TO_FUNC = ImmutableMap.of(
            "=", (v1, v2) -> v1.equals(v2)
    );

    private final Predicate<String> value;

    private NakadiPredicate(final Predicate<String> value) {
        this.value = value;
    }

    public Predicate<String> getValue() {
        return value;
    }

    public static Optional<NakadiPredicate> valueOf(final String expression) {
        final String[] params = expression.split("\\s+");

        if (params.length != WORDS_IN_PREDICATE) {
            LOG.debug("Expression could not be parsed");
            return Optional.empty();
        }

        final String type = params[0];
        final String firstParam = params[1];
        final String operation = params[2];
        final String secondParam = params[3];

        if (!Objects.equals(type, FILTER) || !OPERATION_INT_TO_FUNC.containsKey(operation)) {
            LOG.debug("Expression could not be parsed");
            return Optional.empty();
        }

        return Optional.of(new NakadiPredicate(
                v -> {
                    v = Utils.getObjectFromJsonPath(firstParam, v);
                    if (v == null)
                        return false;

                    if (NumberUtils.isDigits(v))
                        return OPERATION_INT_TO_FUNC.get(operation)
                                .apply(Integer.valueOf(v), Integer.valueOf(secondParam));
                    return OPERATION_STR_TO_FUNC.get(operation).apply(v, secondParam);
                }));
    }
}
