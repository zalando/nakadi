package org.zalando.nakadi.service;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.PaginationWrapper;
import org.zalando.nakadi.repository.db.SchemaRepository;

import javax.ws.rs.core.Response;

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
        final Result<?> result = schemaService.getSchemas("name", -1, 1);
        Assert.assertFalse(result.isSuccessful());
        Assert.assertEquals(Response.Status.BAD_REQUEST, result.getProblem().getStatus());
        Assert.assertEquals("'offset' parameter can't be lower than 0", result.getProblem().getDetail().get());
    }

    @Test
    public void testLimitLowerBounds() {
        final Result<?> result = schemaService.getSchemas("name", 0, 0);
        Assert.assertFalse(result.isSuccessful());
        Assert.assertEquals(Response.Status.BAD_REQUEST, result.getProblem().getStatus());
        Assert.assertEquals("'limit' parameter should have value from 1 to 1000",result.getProblem().getDetail().get());
    }

    @Test
    public void testLimitUpperBounds() {
        final Result<?> result = schemaService.getSchemas("name", 0, 1001);
        Assert.assertFalse(result.isSuccessful());
        Assert.assertEquals(Response.Status.BAD_REQUEST, result.getProblem().getStatus());
        Assert.assertEquals("'limit' parameter should have value from 1 to 1000",result.getProblem().getDetail().get());
    }

    @Test
    public void testSuccess() {
        final Result<PaginationWrapper> result = (Result<PaginationWrapper>) schemaService.getSchemas("name", 0, 1000);
        Assert.assertTrue(result.isSuccessful());
    }

}