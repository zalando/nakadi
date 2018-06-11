package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;
import org.zalando.nakadi.exceptions.runtime.InvalidOffsetException;
import org.zalando.nakadi.repository.db.SchemaRepository;

public class SchemaServiceTest {

    private SchemaRepository schemaRepository;
    private PaginationService paginationService;
    private SchemaService schemaService;

    @Before
    public void setUp() {
        schemaRepository = Mockito.mock(SchemaRepository.class);
        paginationService = Mockito.mock(PaginationService.class);
        schemaService = new SchemaService(schemaRepository, paginationService);
    }

    @Test
    public void testOffsetBounds() {
        try {
            final PaginationWrapper result = schemaService.getSchemas("name", -1, 1);
        } catch (InvalidOffsetException e) {
            Assert.assertEquals("'offset' parameter can't be lower than 0", e.getMessage());
            return;
        }
        Assert.assertFalse(true);
    }

    @Test
    public void testLimitLowerBounds() {
        try {
            final PaginationWrapper result = schemaService.getSchemas("name", 0, 0);
        } catch (InvalidLimitException e) {
            Assert.assertEquals("'limit' parameter should have value from 1 to 1000", e.getMessage());
            return;
        }
        Assert.assertTrue(false);
    }

    @Test
    public void testLimitUpperBounds() {
        try {
            final PaginationWrapper result = schemaService.getSchemas("name", 0, 1001);
        } catch (InvalidLimitException e) {
            Assert.assertEquals("'limit' parameter should have value from 1 to 1000", e.getMessage());
            return;
        }
        Assert.assertTrue(false);
    }
}