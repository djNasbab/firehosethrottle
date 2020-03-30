package com.aws.firehose.walle.pusher.pusher;

import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.StepCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.model.*;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Configuration
public class Config {
    private Logger logger = LoggerFactory.getLogger(Config.class);

    @Bean
    FirehoseAsyncClient firehoseAsyncClient() {
        return FirehoseAsyncClient.builder().region(Region.EU_WEST_1).build();
    }

    @Bean
    String streamName() {
        return "TestStream";
    }

    @Bean
    RecordBatch recordBatch() {
        return new RecordBatch();
    }

    @Bean
    Counter throttledCounter() {
        return new BasicCounter(MonitorConfig.builder("fhose-throttled").build());
    }

    @Bean
    Counter failedCounter() {
        return new BasicCounter(MonitorConfig.builder("fhose-failed").build());
    }

    @Bean
    Counter sentMessageCounter() {
        return new StepCounter(MonitorConfig.builder("fhose-sent").build());
    }


    @Bean
    @Autowired
    String createDeliveryStream(@NonNull FirehoseAsyncClient firehoseAsyncClient,
                                @Value("${fhose.deliveryStreamName}") String streamName,
                                @Value("${fhose.bucketARN}") String bucketARN,
                                @Value("${fhose.roleARN}") String roleArn) {
        try {
            final ListDeliveryStreamsResponse listDeliveryStreamsResponse = firehoseAsyncClient.listDeliveryStreams(ListDeliveryStreamsRequest.builder().build()).get();
            for (final String deliveryStreamName : listDeliveryStreamsResponse.deliveryStreamNames()) {
                if (deliveryStreamName.equals(streamName)) {
                    logger.info("using stream: " + streamName);
                    return streamName;
                }
            }
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        final CompletableFuture<CreateDeliveryStreamResponse> testStream = firehoseAsyncClient.createDeliveryStream(CreateDeliveryStreamRequest
                .builder()
                .deliveryStreamName(streamName)
                .s3DestinationConfiguration(S3DestinationConfiguration.builder()
                        .bucketARN(bucketARN)
                        .roleARN(roleArn)
                        .build())
                .build());

        CreateDeliveryStreamResponse createDeliveryStreamResponse = null;

        try {
            createDeliveryStreamResponse = testStream.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

        return createDeliveryStreamResponse.deliveryStreamARN();
    }
}
