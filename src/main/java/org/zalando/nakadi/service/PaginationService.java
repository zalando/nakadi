package org.zalando.nakadi.service;

import org.springframework.stereotype.Component;
import org.springframework.web.util.UriComponentsBuilder;
import org.zalando.nakadi.domain.PaginationLinks;
import org.zalando.nakadi.domain.PaginationWrapper;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

@Component
public class PaginationService {

    public PaginationWrapper paginate(final List items, final int offset, final int limit,
                                          final Supplier<Integer> countSupplier) {
        final PaginationLinks paginationLinks;
        if (items.isEmpty()) {
            final int count = countSupplier.get();
            final int latestOffset = offset > count ? count : count / offset;
            final int latestLimit = limit > count ? count : count / limit;
            paginationLinks = createLinks(latestOffset, latestLimit, items.size());
        } else {
            paginationLinks = createLinks(offset, limit, items.size());
            if (items.size() > limit)
                items.remove(items.size() - 1);
        }

        return new PaginationWrapper(items, paginationLinks);
    }

    private PaginationLinks createLinks(final int offset, final int limit, final int currentPageItemCount) {
        Optional<PaginationLinks.Link> prevLink = Optional.empty();
        if (offset > 0) {
            final int newOffset = offset >= limit ? offset - limit : 0;
            final String prevUri = UriComponentsBuilder.fromPath("/schemas")
                    .queryParam("offset", newOffset).queryParam("limit", limit).build().toString();
            prevLink = Optional.of(new PaginationLinks.Link(prevUri));
        }

        Optional<PaginationLinks.Link> nextLink = Optional.empty();
        if (currentPageItemCount > limit) {
            final String nextUri = UriComponentsBuilder.fromPath("/schemas")
                    .queryParam("offset", offset + limit).queryParam("limit", limit).build().toString();
            nextLink = Optional.of(new PaginationLinks.Link(nextUri));
        }

        return new PaginationLinks(prevLink, nextLink);
    }

}
