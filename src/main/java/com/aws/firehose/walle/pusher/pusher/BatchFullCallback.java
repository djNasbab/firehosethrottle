package com.aws.firehose.walle.pusher.pusher;

import software.amazon.awssdk.services.firehose.model.Record;

import java.util.List;

/**
 * Simple interface for getting a callback when a batch is full of records.
 */
public interface BatchFullCallback {
    void batchIsComplete(final List<Record> records);
}
