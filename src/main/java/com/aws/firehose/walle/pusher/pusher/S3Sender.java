package com.aws.firehose.walle.pusher.pusher;

import com.netflix.servo.monitor.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.firehose.FirehoseAsyncClient;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.Record;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Component
public class S3Sender {
    private Logger logger = LoggerFactory.getLogger(S3Sender.class);

    private final FirehoseAsyncClient firehoseAsyncClient;

    private final FirehoseMessageHandler firehoseMessageHandler;

    private final Counter sentMessageCounter;

    @Value("${fhose.deliveryStreamName}")
    private String streamName;

    public S3Sender(final FirehoseAsyncClient firehoseAsyncClient, final FirehoseMessageHandler firehoseMessageHandler, @Qualifier("sentMessageCounter") final Counter sentMessageCounter) {
        this.firehoseAsyncClient = firehoseAsyncClient;
        this.firehoseMessageHandler = firehoseMessageHandler;
        this.sentMessageCounter = sentMessageCounter;
    }

    void sendRecordsToS3Async(final List<Record> data) {
        sentMessageCounter.increment(data.size());

        final PutRecordBatchRequest request = PutRecordBatchRequest.builder()
                .records(data)
                .deliveryStreamName(streamName)
                .build();

        firehoseAsyncClient.putRecordBatch(request)
                .whenComplete(firehoseMessageHandler.getPutRecordBatchResponseConsumer(request));
    }

    void sendRecordsToS3Sync(final List<Record> data) {
        try {
            sentMessageCounter.increment(data.size());

            final PutRecordBatchRequest request = PutRecordBatchRequest.builder()
                    .records(data)
                    .deliveryStreamName("TestStream")
                    .build();

            final PutRecordBatchResponse putRecordBatchResponse = firehoseAsyncClient.putRecordBatch(request).get();
            firehoseMessageHandler.handleResponse(request, putRecordBatchResponse);
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error while processing Sync messaging", e);
        }
    }
}
