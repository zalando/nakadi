package org.zalando.nakadi.stream.expression;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;

public class Interpreter {

    private static final Set<ExpressionType> TYPES = ImmutableSet.of(
                    ExpressionType.FILTER,
                    ExpressionType.MAP);

    private final List<ExpressionType> positions = new ArrayList<>();
    private final List<InterpreterStack> interpreterStacks = new ArrayList<>();

    public Interpreter() {
        interpreterStacks.add(new InterpreterStack<NakadiPredicate>(expr -> NakadiPredicate.valueOf(expr)));
        interpreterStacks.add(new InterpreterStack<NakadiMapper>(expr -> NakadiMapper.valueOf(expr)));
    }

    public void expr(final List<String> expressions) {
        for (final String expression: expressions) {
            final String[] params = expression.split("\\s+");
            if (params.length == 0 || Objects.isNull(params[0]))
                throw new RuntimeException("Wrong expression: " + expression);

            final ExpressionType type = Enum.valueOf(ExpressionType.class, params[0].toUpperCase());
            if (!TYPES.contains(type))
                throw new RuntimeException("Wrong expression: " + expression);

            positions.add(type);
            boolean interpreted = false;
            for (final InterpreterStack stack: interpreterStacks) {
                if (stack.push(expression)) {
                    interpreted = true;
                    break;
                }
            }

            if (!interpreted) {
                throw new RuntimeException("Wrong expression: " + expression);
            }
        }
    }

    public List<ExpressionType> getPositions() {
        return ImmutableList.copyOf(positions);
    }

    public NakadiPredicate getNextPredicate() {
        return (NakadiPredicate) interpreterStacks.get(0).pop();
    }

    public NakadiMapper getNextMapper() {
        return (NakadiMapper) interpreterStacks.get(1).pop();
    }

    private static class InterpreterStack<T> {
        private final Stack<T> stack = new Stack<>();
        private final Function<String, Optional<T>> evaluator;

        protected InterpreterStack(final Function<String, Optional<T>> evaluator) {
            this.evaluator = evaluator;
        }

        protected boolean push(final String expression) {
            final Optional<T> result = evaluator.apply(expression);
            if (result.isPresent()) {
                stack.push(result.get());
                return true;
            }
            return false;
        }

        protected T pop() {
            return stack.pop();
        }

    }
}
