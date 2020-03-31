package com.aws.firehose.walle.pusher;

import software.amazon.awssdk.services.firehose.model.Record;

import java.util.List;

public interface BatchRetryCallback {

    /**
     * If a batch fails while sending, make sure that we are distributing the batch back to storage
     * @param records
     */
    void retryBatch(List<Record> records);PusherApplication
}
