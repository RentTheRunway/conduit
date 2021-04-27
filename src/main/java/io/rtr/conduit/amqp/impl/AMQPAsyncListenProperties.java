package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;

public class AMQPAsyncListenProperties extends AMQPCommonListenProperties {
    private AMQPAsyncConsumerCallback callback;

    AMQPAsyncListenProperties(AMQPAsyncConsumerCallback callback, String exchange, String queue, int threshold, int prefetchCount, boolean poisonQueueEnabled, boolean purgeOnConnect, boolean dynamicQueueCreation, String poisonPrefix, String dynamicQueueRoutingKey, boolean autoCreateAndBind, String exchangeType, String routingKey) {
        super(exchange, queue, false, threshold, prefetchCount, poisonQueueEnabled, purgeOnConnect, dynamicQueueCreation, poisonPrefix, dynamicQueueRoutingKey, autoCreateAndBind, exchangeType, routingKey);
        this.callback = callback;
    }
    public AMQPAsyncConsumerCallback getCallback() {
        return callback;
    }

}
