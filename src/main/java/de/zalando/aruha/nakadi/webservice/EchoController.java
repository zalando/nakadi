package de.zalando.aruha.nakadi.webservice;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping(value = "/api", produces = MediaType.TEXT_PLAIN)
public class EchoController {

  private static final Logger LOG = LoggerFactory.getLogger(EchoController.class);

  @RequestMapping(value = "/echo", method = GET)
  public String echo(@RequestParam(value = "toEcho") final String toEcho) {
    LOG.info("Echo is called");
    return toEcho;
  }
}
