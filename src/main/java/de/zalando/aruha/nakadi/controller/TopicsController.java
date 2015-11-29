package de.zalando.aruha.nakadi.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.annotation.Timed;

import de.zalando.aruha.nakadi.domain.Problem;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.TopicRepository;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
@RequestMapping(value = "/topics")
public class TopicsController {

    @Autowired
    private TopicRepository topicRepository;

    @Timed
    @RequestMapping(value = "/", method = RequestMethod.GET)
    @ApiOperation("Lists all known topics")

    // FIXME: response for 200 does not match reality
    @ApiResponses(
        {
            @ApiResponse(code = 200, message = "Returns list of all topics", response = Topic.class),
            @ApiResponse(
                code = 401, message = "User not authenticated", response = Problem.class
            ), @ApiResponse(
                code = 503, message = "Not available", response = Problem.class
            )
        }
    )
    public List<Topic> listTopics() {
        return topicRepository.listTopics();
    }

    @Timed
    @RequestMapping(value = "/{topicId}", method = RequestMethod.GET)
    public void getTopic(@PathVariable("topicId") final String topicId) {
        throw new UnsupportedOperationException();
    }

    @Timed
    @RequestMapping(value = "/{topicId}/partitions", method = RequestMethod.GET)
    public List<TopicPartition> listPartitions(@PathVariable("topicId") final String topicId) {
    	return topicRepository.listPartitions(topicId);
    }

    @Timed
    @RequestMapping(value = "/{topicId}/partitions/{partitionId}", method = RequestMethod.GET)
    public TopicPartition getPartition(@PathVariable("topicId") final String topicId) {
        throw new UnsupportedOperationException();
    }

    @Timed
    @RequestMapping(value = "/{topicId}/partitions/{partitionId}", method = RequestMethod.POST)
    public void postMessageToPartition(@PathVariable("topicId") final String topicId,
            @PathVariable("partitionId") final String partitionId, @RequestBody final String v) {
        topicRepository.postMessage(topicId, partitionId, v);
    }

}
