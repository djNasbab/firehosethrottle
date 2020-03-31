package com.aws.firehose.walle.pusher;

public class DeliveryStream {

    public final String deliveryStreamName;

    public DeliveryStream(final String deliveryName) {
        this.deliveryStreamName = deliveryName;
    }

    public String getDeliveryStreamName() {
        return deliveryStreamName;
    }

}
