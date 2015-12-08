package de.zalando.aruha.nakadi.webservice;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
@Api(basePath = "/api", value = "Echo API", description = "Operations with echos", produces = "text/plain")
@RequestMapping(value = "/api", produces = MediaType.TEXT_PLAIN)
public class EchoController {

    private static final Logger LOG = LoggerFactory.getLogger(EchoController.class);

    @ApiOperation(value = "Echo a message")
    @ApiResponses(
        value = {
            @ApiResponse(code = 400, message = "Fields are with validation errors"),
            @ApiResponse(code = 202, message = "Success")
        }
    )
    @RequestMapping(value = "/echo", method = GET)
    public String echo(@RequestParam(value = "toEcho") final String toEcho) {
        LOG.info("Echo is called");
        return toEcho;
    }
}
