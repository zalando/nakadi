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

    public PaginationWrapper paginate(final int offset,
                                      final int limit,
                                      final String path,
                                      final ItemsSupplier itemsSupplier,
                                      final Supplier<Integer> countSupplier) {
        final List items = itemsSupplier.queryOneMore(offset, limit);
        final PaginationLinks paginationLinks;
        if (items.isEmpty() && offset != 0 && limit != 0) {
            final int count = countSupplier.get();
            int latestOffset = count / limit;
            if (offset >= latestOffset) {
                latestOffset = 0;
            }
            paginationLinks = createLinks(path, latestOffset, limit, items.size());
        } else {
            paginationLinks = createLinks(path, offset, limit, items.size());
            if (items.size() > limit) {
                items.remove(items.size() - 1);
            }
        }

        return new PaginationWrapper(items, paginationLinks);
    }

    private PaginationLinks createLinks(final String path,
                                        final int offset,
                                        final int limit,
                                        final int currentPageItemCount) {
        Optional<PaginationLinks.Link> prevLink = Optional.empty();
        if (offset > 0) {
            final int newOffset = offset >= limit ? offset - limit : 0;
            final String prevUri = UriComponentsBuilder.fromPath(path)
                    .queryParam("offset", newOffset).queryParam("limit", limit).build().toString();
            prevLink = Optional.of(new PaginationLinks.Link(prevUri));
        }

        Optional<PaginationLinks.Link> nextLink = Optional.empty();
        if (currentPageItemCount > limit) {
            final String nextUri = UriComponentsBuilder.fromPath(path)
                    .queryParam("offset", offset + limit).queryParam("limit", limit).build().toString();
            nextLink = Optional.of(new PaginationLinks.Link(nextUri));
        }

        return new PaginationLinks(prevLink, nextLink);
    }

    public interface ItemsSupplier {

        List query(int offset, int limit);

        default List queryOneMore(final int offset, final int limit) {
            return query(offset, limit + 1);
        }
    }

}
