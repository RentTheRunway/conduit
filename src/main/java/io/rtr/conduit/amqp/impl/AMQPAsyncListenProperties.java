package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;

public class AMQPAsyncListenProperties extends AMQPCommonListenProperties {
    private AMQPAsyncConsumerCallback callback;

    public AMQPAsyncListenProperties(AMQPAsyncConsumerCallback callback, String exchange, String queue, int threshold, int prefetchCount, boolean poisonQueueEnabled, boolean purgeOnConnect, boolean dynamicQueueCreation, String poisonPrefix, String dynamicQueueRoutingKey) {
        super(exchange, queue, threshold, prefetchCount, poisonQueueEnabled, purgeOnConnect, dynamicQueueCreation, poisonPrefix, dynamicQueueRoutingKey);
        this.callback = callback;
    }
    public AMQPAsyncConsumerCallback getCallback() {
        return callback;
    }

}
