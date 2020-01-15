package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonInclude;

import javax.annotation.concurrent.Immutable;
import java.util.Optional;

@Immutable
@JsonInclude(JsonInclude.Include.NON_ABSENT)
public class PaginationLinks {

    private final Optional<Link> prev;

    private final Optional<Link> next;

    public PaginationLinks() {
        this(Optional.empty(), Optional.empty());
    }

    public PaginationLinks(final Optional<Link> prev, final Optional<Link> next) {
        this.prev = prev;
        this.next = next;
    }

    public Optional<Link> getPrev() {
        return prev;
    }

    public Optional<Link> getNext() {
        return next;
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
