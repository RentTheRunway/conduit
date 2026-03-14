package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.transport.TransportListenProperties;

/**
 * There is a single restriction on the exchange/queue architecture; the consumer queue must have a
 * respective poison counterpart. Given a queue "Q", there must exist a queue "Q.poison".
 */
public class AMQPListenProperties extends AMQPCommonListenProperties
        implements TransportListenProperties {
    private AMQPConsumerCallback callback;

    AMQPListenProperties(
            final AMQPConsumerCallback callback,
            final String exchange,
            final String queue,
            final boolean isAutoDeleteQueue,
            final int threshold,
            final boolean poisonQueueEnabled,
            final boolean purgeOnConnect,
            final int prefetchCount,
            final String poisonPrefix,
            final String dynamicQueueRoutingKey,
            final boolean dynamicQueueCreation,
            final String exchangeType,
            final String routingKey,
            final boolean autoCreateAndBind,
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

    public AMQPConsumerCallback getCallback() {
        return callback;
    }
}
