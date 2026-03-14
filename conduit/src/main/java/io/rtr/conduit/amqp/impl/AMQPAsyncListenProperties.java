package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;

public class AMQPAsyncListenProperties extends AMQPCommonListenProperties {
    private AMQPAsyncConsumerCallback callback;

    AMQPAsyncListenProperties(
            final AMQPAsyncConsumerCallback callback,
            final String exchange,
            final String queue,
            final boolean isAutoDeleteQueue,
            final int threshold,
            final int prefetchCount,
            final boolean poisonQueueEnabled,
            final boolean purgeOnConnect,
            final boolean dynamicQueueCreation,
            final String poisonPrefix,
            final String dynamicQueueRoutingKey,
            final boolean autoCreateAndBind,
            final String exchangeType,
            final String routingKey,
            final boolean exclusive) {
        super(
                exchange,
                queue,
                isAutoDeleteQueue,
                threshold,
                prefetchCount,
                poisonQueueEnabled,
                purgeOnConnect,
                dynamicQueueCreation,
                poisonPrefix,
                dynamicQueueRoutingKey,
                autoCreateAndBind,
                exchangeType,
                routingKey,
                exclusive);
        this.callback = callback;
    }

    public AMQPAsyncConsumerCallback getCallback() {
        return callback;
    }
}
