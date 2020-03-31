package com.aws.firehose.walle.pusher.pusher;

import com.netflix.servo.monitor.BasicCounter;
import com.netflix.servo.monitor.Counter;
import com.netflix.servo.monitor.MonitorConfig;
import com.netflix.servo.monitor.StepCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
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
    @ConditionalOnProperty(
            value="fhose.deliveryStreamType",
            havingValue = "s3")
    DeliveryStream deliveryStream(@NonNull FirehoseAsyncClient firehoseAsyncClient,
                                  @Value("${fhose.deliveryStreamName}") String streamName,
                                  @NonNull S3DestinationConfiguration s3DestinationConfiguration) {
        try {
            //Check if stream is already created

            DeliveryStream deliveryStream = getDeliveryStream(firehoseAsyncClient, streamName);

            if (deliveryStream != null) {
                return deliveryStream;
            }

            CreateDeliveryStreamResponse testStream = firehoseAsyncClient.createDeliveryStream(CreateDeliveryStreamRequest
                    .builder()
                    .deliveryStreamName(streamName)
                    .s3DestinationConfiguration(s3DestinationConfiguration)
                    .build())
                    .get();

            return new DeliveryStream(streamName);

        } catch (InterruptedException | ExecutionException e) {
            logger.error("Execution error: ", e);
        }

        return new DeliveryStream(streamName);
    }

    @Bean
    @ConditionalOnProperty(
            value="fhose.deliveryStreamType",
            havingValue = "s3")
    S3DestinationConfiguration createS3DestinationConfiguration(@Value("${fhose.s3.bucketARN}") String bucketARN,
                                                                @Value("${fhose.s3.roleARN}") String roleArn) {
        return S3DestinationConfiguration.builder()
                .bucketARN(bucketARN)
                .roleARN(roleArn)
                .build();
    }

    @Bean
    @ConditionalOnProperty(
            value="${fhose.deliveryStreamType}",
            havingValue = "aes")
    DeliveryStream createAESDeliveryStream(@NonNull FirehoseAsyncClient firehoseAsyncClient,
                                           @Value("${fhose.deliveryStreamName}") String streamName,
                                           @Value("${fhose.indexName}") String indexName,
                                           @Value("${fhose.domainARN}") String domainARN,
                                           @Value("${fhose.rotationPeriod}") String rotationPeriod) {
        try {

            //Check if stream is already created
            DeliveryStream deliveryStream = getDeliveryStream(firehoseAsyncClient, streamName);

            if (deliveryStream != null) {
                return deliveryStream;
            }

            final CompletableFuture<CreateDeliveryStreamResponse> aesStream = firehoseAsyncClient.createDeliveryStream(CreateDeliveryStreamRequest
                    .builder()
                    .deliveryStreamName(streamName)
                    .elasticsearchDestinationConfiguration(ElasticsearchDestinationConfiguration.builder()
                            .indexName(indexName)
                            .domainARN(domainARN)
                            .indexRotationPeriod(ElasticsearchIndexRotationPeriod.fromValue(rotationPeriod))
                            .s3BackupMode(ElasticsearchS3BackupMode.ALL_DOCUMENTS)
                            .build())
                    .build());

            aesStream.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Execution error: ", e);
        }
        return new DeliveryStream(streamName);
    }

    private DeliveryStream getDeliveryStream(final FirehoseAsyncClient firehoseAsyncClient, final String streamName){
        try {
            final ListDeliveryStreamsResponse listDeliveryStreamsResponse = firehoseAsyncClient.listDeliveryStreams(ListDeliveryStreamsRequest.builder().build()).get();
            for (final String deliveryStreamName : listDeliveryStreamsResponse.deliveryStreamNames()) {
                if (deliveryStreamName.equals(streamName)) {
                    logger.info("using stream: " + streamName);
                    return new DeliveryStream(streamName);
                }
            }
            return null;
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Execution error: ", e);
            return null;
        }
    }

}
