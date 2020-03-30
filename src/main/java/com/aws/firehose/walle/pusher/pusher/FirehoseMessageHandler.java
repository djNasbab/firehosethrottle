package com.aws.firehose.walle.pusher.pusher;

import com.netflix.servo.monitor.Counter;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchRequest;
import software.amazon.awssdk.services.firehose.model.PutRecordBatchResponse;
import software.amazon.awssdk.services.firehose.model.Record;

import java.util.List;
import java.util.function.BiConsumer;

@Component
public class FirehoseMessageHandler {

    private static Logger LOGGER = LoggerFactory.getLogger(FirehoseMessageHandler.class);

    private static final String THROTTLED_ERROR_CODE = "ServiceUnavailableException";

    private static final String THROTTLED_MESSAGE = "Slow down.";

    static final String INTERNAL_FAILURE_ERROR_CODE = "InternalFailure";

    private static final String MESSAGE_AND_RECORD_ID = "Error Message: {} Record ID: {}";

    private final Counter throttledCounter;

    private final Counter failedCounter;

    private BatchRetryCallback callback;

    public FirehoseMessageHandler(final @Qualifier("failedCounter") Counter failedCounter, final @Qualifier("throttledCounter") Counter throttledCounter) {
        this.throttledCounter = throttledCounter;
        this.failedCounter = failedCounter;
    }

    public void setCallback(final BatchRetryCallback callback) {
        this.callback = callback;
    }

    @NotNull BiConsumer<PutRecordBatchResponse, Throwable> getPutRecordBatchResponseConsumer(@NonNull PutRecordBatchRequest recordBatchRequest) {
        return (putRecordBatchResponse, throwable) -> {
            if (throwable != null) {
                LOGGER.info("Error while processing Response From Request on stream {}", recordBatchRequest.deliveryStreamName() , throwable);
            } else {
                //Got Batch response, lets look if we have any failed records.
                if (putRecordBatchResponse.failedPutCount() != 0) {
                    failedRecords(recordBatchRequest, putRecordBatchResponse);
                }
            }
        };
    }

    /**
     * Helper method for the sync api to call.
     * @param recordBatchRequest
     * @param putRecordBatchResponse
     */
    public void handleResponse(final PutRecordBatchRequest recordBatchRequest,
                               final PutRecordBatchResponse putRecordBatchResponse) {
        if (putRecordBatchResponse.failedPutCount() != 0) {
            failedRecords(recordBatchRequest, putRecordBatchResponse);
        }
    }

    /**
     * If a response from Firehose returns with a non 0 value on the failedPutCount().
     * @param recordRequest
     * @param response
     */
    private void failedRecords(PutRecordBatchRequest recordRequest, PutRecordBatchResponse response) {
        response.requestResponses().stream()
                .filter(putRecordBatchResponseEntry -> !putRecordBatchResponseEntry.errorCode().equals(Strings.EMPTY))
                .forEach(putRecordBatchResponseEntry -> {
                    LOGGER.error(MESSAGE_AND_RECORD_ID, putRecordBatchResponseEntry.errorMessage(), putRecordBatchResponseEntry.recordId());
                    switch (putRecordBatchResponseEntry.errorCode()) {
                        case THROTTLED_ERROR_CODE:
                            if (THROTTLED_MESSAGE.equals(putRecordBatchResponseEntry.errorMessage())) {
                                throttledCounter.increment();
                            }
                        case INTERNAL_FAILURE_ERROR_CODE:
                            failedCounter.increment();
                            setRetry(recordRequest.records());
                            //Lets retry these as they are voided.
                    }
                });
    }

    private void setRetry(List<Record> records) {
        callback.retryBatch(records);
    }
}
