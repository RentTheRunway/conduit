package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;

public class AMQPAsyncListenProperties extends AMQPCommonListenProperties {
    private AMQPAsyncConsumerCallback callback;

    AMQPAsyncListenProperties(AMQPAsyncConsumerCallback callback, String exchange, String queue, boolean isAutoDeleteQueue, int threshold, int prefetchCount, boolean poisonQueueEnabled, boolean purgeOnConnect, boolean dynamicQueueCreation, String poisonPrefix, String dynamicQueueRoutingKey, boolean autoCreateAndBind, String exchangeType, String routingKey, boolean exclusive) {
        super(exchange, queue, isAutoDeleteQueue, threshold, prefetchCount, poisonQueueEnabled, purgeOnConnect, dynamicQueueCreation, poisonPrefix, dynamicQueueRoutingKey, autoCreateAndBind, exchangeType, routingKey, exclusive);
        this.callback = callback;
    }
    public AMQPAsyncConsumerCallback getCallback() {
        return callback;
    }

}
