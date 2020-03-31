package com.aws.firehose.walle.pusher;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.firehose.model.Record;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class RecordBatch {

    /**
     * Maximum batch size in bytes; see https://docs.aws.amazon.com/firehose/latest/dev/limits.html to read that "The
     * PutRecordBatch operation can take up to 500 records per call or 4 MiB per call, whichever is smaller."
     */
    int MAX_BYTES_IN_BATCH = 4 * 1024 * 1024; // 4 MiB

    /**
     * Maximum record size in bytes; see https://docs.aws.amazon.com/firehose/latest/dev/limits.html to read that "The
     * maximum size of a record sent to Kinesis Data Firehose, before base64-encoding, is 1,000 KiB."
     */
    int MAX_BYTES_IN_RECORD = 1024 * 1000; // 1 KiB

    /**
     * Maximum number of Records allowed in a batch; see https://docs.aws.amazon.com/firehose/latest/dev/limits.html to
     * read that "The PutRecordBatch operation can take up to 500 records per call or 4 MiB per call, whichever is
     * smaller."
     */
    int MAX_RECORDS_IN_BATCH = 100;

    /**
     * Max time allowed for buffering
     */
    long LAST_BATCH_TIME_DIFF_ALLOWED_MILLIS = 3000;

    /**
     * List of records
     */
    private List<Record> records;

    /**
     * Timestamp for when last batch was pulled from the list
     */
    private long lastProcessedTimeStamp = 0;

    /**
     * Callback handler for batch
     */
    private Consumer<List<Record>> recordConsumer;

    public RecordBatch() {
        initialize();
    }

    private void initialize() {
        records = new ArrayList<>(MAX_RECORDS_IN_BATCH);
    }

    private boolean shouldCreateNewBatchDueToRecordCount() {
        return records.size() == MAX_RECORDS_IN_BATCH;
    }

    public void addRecord(final String data) {
        final Record record = Record.builder().data(SdkBytes.fromUtf8String(data)).build();
        if (shouldCreateNewBatch()) {
            onFullRecord(this.records);
            initialize();
            addRecord(record);
        } else {
            addRecord(record);
        }
    }

    private boolean shouldCreateNewBatch() {
        return shouldCreateNewBatchDueToRecordCount() || batchCreationTimedOut();
    }

    private boolean batchCreationTimedOut(){
        if( lastProcessedTimeStamp != 0) {
            long now = System.currentTimeMillis();
            if ((now - lastProcessedTimeStamp) > LAST_BATCH_TIME_DIFF_ALLOWED_MILLIS) {
                lastProcessedTimeStamp = now;
                return true;
            }
        }
        return false;
    }


    /**
     * If no data is coming for a while, periodically flush the buffer to make sure that we arent
     */
    void flushRecords() {
        if (!records.isEmpty()) {
            if (shouldCreateNewBatch()) {
                onFullRecord(this.records);
            }
        }
    }

    void setBatchConsumer(final Consumer<List<Record>> recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    private void onFullRecord(List<Record> records){
        recordConsumer.accept(records);
    }

    private void addRecord(Record record) {
        records.add(record);
    }
}
