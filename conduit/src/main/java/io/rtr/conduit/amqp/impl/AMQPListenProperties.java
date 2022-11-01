package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.AMQPConsumerCallback;
import io.rtr.conduit.amqp.transport.TransportListenProperties;

/**
 * There is a single restriction on the exchange/queue architecture; the consumer
 * queue must have a respective poison counterpart. Given a queue "Q", there must
 * exist a queue "Q.poison".
 */
public class AMQPListenProperties extends AMQPCommonListenProperties implements TransportListenProperties {
    private AMQPConsumerCallback callback;

    AMQPListenProperties(AMQPConsumerCallback callback, String exchange, String queue, boolean isAutoDeleteQueue, int threshold, boolean poisonQueueEnabled, boolean purgeOnConnect, int prefetchCount, String poisonPrefix, String dynamicQueueRoutingKey, boolean dynamicQueueCreation, String exchangeType, String routingKey, boolean autoCreateAndBind, boolean exclusive) {
        super(exchange, queue, isAutoDeleteQueue, threshold, prefetchCount, poisonQueueEnabled, purgeOnConnect, dynamicQueueCreation, poisonPrefix, dynamicQueueRoutingKey, autoCreateAndBind, exchangeType, routingKey, exclusive);
        this.callback = callback;
    }

    public AMQPConsumerCallback getCallback() {
        return callback;
    }
}
