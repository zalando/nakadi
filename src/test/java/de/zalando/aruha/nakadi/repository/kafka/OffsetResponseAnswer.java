package de.zalando.aruha.nakadi.repository.kafka;

import static org.mockito.Mockito.when;

import static scala.collection.JavaConverters.mapAsJavaMapConverter;

import java.util.Map;
import java.util.Set;

import org.mockito.Mockito;

import org.mockito.invocation.InvocationOnMock;

import org.mockito.stubbing.Answer;

import kafka.api.PartitionOffsetRequestInfo;

import kafka.common.TopicAndPartition;

import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;

class OffsetResponseAnswer implements Answer<OffsetResponse> {
    private final Set<PartitionState> partitionStates;

    OffsetResponseAnswer(final Set<PartitionState> partitionStates) {
        this.partitionStates = partitionStates;
    }

    @Override
    public OffsetResponse answer(final InvocationOnMock invocation) throws Throwable {
        final OffsetRequest offsetRequest = (OffsetRequest) invocation.getArguments()[0];

        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestMap =
            (Map<TopicAndPartition, PartitionOffsetRequestInfo>) mapAsJavaMapConverter(offsetRequest.underlying()
                    .requestInfo()).asJava();
/*
 *      final Map<TopicAndPartition, PartitionOffsetsResponse> responseMap;
 *      responseMap = requestMap.entrySet().stream().collect(Collectors.toMap(entry -> entry.getKey(),
 *                  entry -> {
 *                      final PartitionState partitionState = getPartitionInfoFor(entry.getKey());
 *                      final PartitionOffsetRequestInfo requestInfo = entry.getValue();
 *
 *                      final Long offset = offsetFor(partitionState, requestInfo);
 *
 *                      final Buffer<Object> offsets = JavaConversions.asScalaBuffer(Arrays.asList(offset));
 *                      return new PartitionOffsetsResponse((short) 0, offsets);
 *                  }));
 *
 *
 *      final scala.collection.mutable.Map<TopicAndPartition, PartitionOffsetsResponse> scalaResponseMap =
 *          (scala.collection.mutable.Map<TopicAndPartition, PartitionOffsetsResponse>) mapAsScalaMapConverter(
 *              responseMap).asScala();
 */
        final OffsetResponse offsetResponse = Mockito.mock(OffsetResponse.class);

        requestMap.entrySet().stream().forEach(entry -> {
            final PartitionState partitionState = getPartitionInfoFor(entry.getKey());
            final PartitionOffsetRequestInfo requestInfo = entry.getValue();

            final Long offset = offsetFor(partitionState, requestInfo);

            when(offsetResponse.offsets(partitionState.topic, partitionState.partition))
                .thenReturn(new long[] {offset});
        });

        return offsetResponse;
    }

    private Long offsetFor(final PartitionState partitionState, final PartitionOffsetRequestInfo requestInfo) {
        if (requestInfo.time() == KafkaRepositoryTest.EARLIEST_TIME) {
            return partitionState.earliestOffset;
        } else if (requestInfo.time() == KafkaRepositoryTest.LATEST_TIME) {
            return partitionState.latestOffset;
        } else {
            throw new RuntimeException("Cannot handle offset request");
        }
    }

    private PartitionState getPartitionInfoFor(final TopicAndPartition topicAndPartition) {
        return partitionStates.stream()
                              .filter((pi) ->
                                      pi.topic.equals(topicAndPartition.topic())
                                      && pi.partition == topicAndPartition.partition())
                              .findFirst()
                              .orElseThrow(() ->
                                      new RuntimeException("No such TopicAndPartition: " + topicAndPartition));
    }
}
