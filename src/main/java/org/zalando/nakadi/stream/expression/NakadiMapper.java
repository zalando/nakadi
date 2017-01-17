package org.zalando.nakadi.stream.expression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.stream.Utils;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * map warehouse.tools
 *  ^          ^
 * type    firstParam
 */
public class NakadiMapper {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiMapper.class);
    private static final int WORDS_IN_PREDICATE = 2;
    private static final String MAP = ExpressionType.MAP.toString().toLowerCase();

    private final Function<String, String> value;

    private NakadiMapper(final Function<String, String> value) {
        this.value = value;
    }

    public static Optional<NakadiMapper> valueOf(final String expression) {
        final String[] params = expression.split("\\s+");

        if (params.length != WORDS_IN_PREDICATE) {
            LOG.debug("Expression could not be parsed");
            return Optional.empty();
        }

        final String type = params[0];
        final String firstParam = params[1];

        if (!Objects.equals(type, MAP)) {
            LOG.debug("Expression could not be parsed");
            return Optional.empty();
        }

        return Optional.of(new NakadiMapper(v -> {
            final String newv = Utils.getObjectFromJsonPath(firstParam, v);
            if (newv == null)
                return v;
            return newv;
        }));
    }

    public Function<String, String> getValue() {
        return value;
    }
}
