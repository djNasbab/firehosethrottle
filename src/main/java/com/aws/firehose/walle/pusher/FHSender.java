package com.aws.firehose.walle.pusher;

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
public class FHSender {
    private Logger logger = LoggerFactory.getLogger(FHSender.class);

    private final FirehoseAsyncClient firehoseAsyncClient;

    private final FirehoseMessageHandler firehoseMessageHandler;

    private final Counter sentMessageCounter;

    @Value("${fhose.deliveryStreamName}")
    private String streamName;

    public FHSender(final FirehoseAsyncClient firehoseAsyncClient,
                    final FirehoseMessageHandler firehoseMessageHandler,
                    @Qualifier("sentMessageCounter") final Counter sentMessageCounter) {
        this.firehoseAsyncClient = firehoseAsyncClient;
        this.firehoseMessageHandler = firehoseMessageHandler;
        this.sentMessageCounter = sentMessageCounter;
    }

    void sendRecordsToFHAsync(final List<Record> data) {
        sentMessageCounter.increment(data.size());

        final PutRecordBatchRequest request = PutRecordBatchRequest.builder()
                .records(data)
                .deliveryStreamName(streamName)
                .build();

        firehoseAsyncClient.putRecordBatch(request)
                .whenComplete(firehoseMessageHandler.getPutRecordBatchResponseConsumer(request));
    }

    PutRecordBatchResponse sendRecordsToFHSync(final List<Record> data) {
        try {
            sentMessageCounter.increment(data.size());

            final PutRecordBatchRequest request = PutRecordBatchRequest.builder()
                    .records(data)
                    .deliveryStreamName("TestStream")
                    .build();

            final PutRecordBatchResponse putRecordBatchResponse = firehoseAsyncClient.putRecordBatch(request).get();
            firehoseMessageHandler.handleResponse(request, putRecordBatchResponse);
            return putRecordBatchResponse;
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error while processing Sync messaging", e);
            return null;
        }
    }
}
