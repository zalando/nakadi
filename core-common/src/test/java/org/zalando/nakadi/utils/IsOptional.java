package org.zalando.nakadi.utils;

import com.google.common.base.Objects;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.annotation.Nonnull;
import java.util.Optional;

public class IsOptional<T> extends TypeSafeMatcher<Optional<? extends T>> {

    public static IsOptional<Object> isAbsent() {
        return new IsOptional<>(false);
    }

    public static IsOptional<Object> isPresent() {
        return new IsOptional<>(true);
    }

    public static <T> IsOptional<T> isValue(final T value) {
        return new IsOptional<T>(value);
    }

    public static <T> IsOptional<T> matches(final Matcher<T> matcher) {
        return new IsOptional<>(matcher);
    }

    private final boolean someExpected;

    @Nonnull
    private final Optional<T> expected;

    @Nonnull
    private final Optional<Matcher<T>> matcher;

    private IsOptional(final boolean someExpected) {
        this.someExpected = someExpected;
        this.expected = Optional.empty();
        this.matcher = Optional.empty();
    }

    private IsOptional(final T value) {
        this.someExpected = true;
        this.expected = Optional.of(value);
        this.matcher = Optional.empty();
    }

    private IsOptional(final Matcher<T> matcher) {
        this.someExpected = true;
        this.expected = Optional.empty();
        this.matcher = Optional.of(matcher);
    }

    @Override
    public void describeTo(final Description description) {
        if (!someExpected) {
            description.appendText("<Absent>");
        } else if (expected.isPresent()) {
            description.appendValue(expected);// "a Some with " +
            // expected.some());
        } else if (matcher.isPresent()) {
            description.appendText("a present value matching ");
            matcher.get().describeTo(description);
        } else {
            description.appendText("<Present>");
        }
    }

    @Override
    public boolean matchesSafely(final Optional<? extends T> item) {
        if (!someExpected) {
            return !item.isPresent();
        } else if (expected.isPresent()) {
            return item.isPresent() && Objects.equal(item.get(), expected.get());
        } else if (matcher.isPresent()) {
            return item.isPresent() && matcher.get().matches(item.get());
        } else {
            return item.isPresent();
        }
    }
}