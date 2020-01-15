package org.zalando.nakadi.service;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.PaginationWrapper;

import java.util.Collections;

public class PaginationServiceTest {

    @Test
    public void testPaginationPrev() {
        final PaginationService paginationService = new PaginationService();
        final PaginationWrapper paginationWrapper =
                paginationService.paginate(2, 5, "/schemas", (o, l) -> Collections.emptyList(), () -> 1);
        Assert.assertFalse(paginationWrapper.getLinks().getNext().isPresent());
        Assert.assertFalse(paginationWrapper.getLinks().getPrev().isPresent());
    }

    @Test
    public void testPaginationPrev2() {
        final PaginationService paginationService = new PaginationService();
        final PaginationWrapper paginationWrapper =
                paginationService.paginate(2, 5, "/schemas", (o, l) -> Collections.emptyList(), () -> 20);
        Assert.assertFalse(paginationWrapper.getLinks().getNext().isPresent());
        Assert.assertEquals("/schemas?offset=0&limit=5", paginationWrapper.getLinks().getPrev().get().getHref());
    }

    @Test
    public void testPaginationPrev3() {
        final PaginationService paginationService = new PaginationService();
        final PaginationWrapper paginationWrapper =
                paginationService.paginate(2, 5, "/schemas", (o, l) -> Collections.emptyList(), () -> 2);
        Assert.assertFalse(paginationWrapper.getLinks().getNext().isPresent());
        Assert.assertFalse(paginationWrapper.getLinks().getPrev().isPresent());
    }

    @Test
    public void testPaginationPrev4() {
        final PaginationService paginationService = new PaginationService();
        final PaginationWrapper paginationWrapper =
                paginationService.paginate(2, 5, "/schemas", (o, l) -> Collections.emptyList(), () -> 5);
        Assert.assertFalse(paginationWrapper.getLinks().getNext().isPresent());
        Assert.assertFalse(paginationWrapper.getLinks().getPrev().isPresent());
    }

    @Test
    public void testPaginationEmpty() {
        final PaginationService paginationService = new PaginationService();
        final PaginationWrapper paginationWrapper =
                paginationService.paginate(0, 5, "/schemas", (o, l) -> Collections.emptyList(), () -> 5);
        Assert.assertFalse(paginationWrapper.getLinks().getNext().isPresent());
        Assert.assertFalse(paginationWrapper.getLinks().getPrev().isPresent());
    }

    @Test
    public void testPaginationNext() {
        final PaginationService paginationService = new PaginationService();
        final PaginationWrapper paginationWrapper =
                paginationService.paginate(0, 3, "/schemas",
                        (o, l) -> Lists.newArrayList("One", "Two", "Three", "Four"),
                        () -> 1);
        Assert.assertEquals("/schemas?offset=3&limit=3", paginationWrapper.getLinks().getNext().get().getHref());
        Assert.assertFalse(paginationWrapper.getLinks().getPrev().isPresent());
    }

    @Test
    public void testPaginationPrevAndNext() {
        final PaginationService paginationService = new PaginationService();
        final PaginationWrapper paginationWrapper =
                paginationService.paginate(2, 3, "/schemas",
                        (o, l) -> Lists.newArrayList("One", "Two", "Three", "Four"),
                        () -> 1);
        Assert.assertEquals("/schemas?offset=5&limit=3", paginationWrapper.getLinks().getNext().get().getHref());
        Assert.assertEquals("/schemas?offset=0&limit=3", paginationWrapper.getLinks().getPrev().get().getHref());
    }
}