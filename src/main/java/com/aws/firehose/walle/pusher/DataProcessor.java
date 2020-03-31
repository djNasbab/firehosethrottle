package com.aws.firehose.walle.pusher;

import com.netflix.servo.monitor.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

@Component
@DependsOn({"FHSender", "deliveryStream"})
public class DataProcessor {
    private static Logger LOGGER = LoggerFactory.getLogger(DataProcessor.class);

    private final RecordBatch recordBatch;

    private final Counter failedCounter;

    private final Counter throttledCounter;

    private final Counter sentMessageCounter;

    private static final String LOGGED_METRIC = "Avg Second Sent Messages: {} Throttled Messages TOTAL: {} Failed Message TOTAL: {}";

    private ScheduledExecutorService executorService = Executors.newScheduledThreadPool(1);

    public DataProcessor(final FHSender FHSender, final RecordBatch recordBatch,
                         final @Qualifier("failedCounter") Counter failedCounter,
                         final @Qualifier("throttledCounter") Counter throttledCounter,
                         final @Qualifier("sentMessageCounter") Counter sentMessageCounter) {
        this.recordBatch = recordBatch;
        this.failedCounter = failedCounter;
        this.throttledCounter = throttledCounter;
        this.sentMessageCounter = sentMessageCounter;

        //Setup the listener for when the batch is full and ready to send OR the timeout has been met ( 5 seconds )
        recordBatch.setBatchConsumer(FHSender::sendRecordsToFHAsync);
    }

    @PostConstruct
    public void startThread() {
        //Scheduele to send 2 batches a second
        executorService.scheduleAtFixedRate(this::sendData, 1000, 500, TimeUnit.MILLISECONDS);

        //Scheduele Counter printout
        executorService.scheduleAtFixedRate(this::printStats, 60, 10, TimeUnit.SECONDS);

        //Flush it ever 10 seconds if no data is arriving
        executorService.scheduleAtFixedRate(recordBatch::flushRecords, 30, 10, TimeUnit.SECONDS);
    }

    private void sendData() {
        for (int i = 0; i < 20; i++) {
            recordBatch.addRecord(createTestData());
        }
    }

    private void printStats() {
        LOGGER.info(LOGGED_METRIC,sentMessageCounter.getValue().intValue(),
                throttledCounter.getValue().intValue(),
                failedCounter.getValue().intValue());
    }

    private static String createTestData() {
        return ThreadLocalRandom.current().nextInt() + "\n";
    }
}