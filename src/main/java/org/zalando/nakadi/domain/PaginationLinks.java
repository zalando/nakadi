package org.zalando.nakadi.domain;

import java.util.Optional;

import javax.annotation.concurrent.Immutable;

import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Immutable
@JsonInclude(JsonInclude.Include.NON_ABSENT)
@Getter
@AllArgsConstructor
public class PaginationLinks {

    private final Optional<Link> prev;

    private final Optional<Link> next;

    public PaginationLinks() {
        this(Optional.empty(), Optional.empty());
    }

    @Immutable
    public static class Link {

        private final String href;

        public Link(final String href) {
            this.href = href;
        }

        public String getHref() {
            return href;
        }
    }
}
