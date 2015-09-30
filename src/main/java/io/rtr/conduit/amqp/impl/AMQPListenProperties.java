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

    AMQPListenProperties(AMQPConsumerCallback callback, String exchange, String queue, int threshold, int prefetchCount, boolean poisonQueueEnabled, boolean purgeOnConnect, boolean dynamicQueueCreation, String poisonPrefix, String dynamicQueueRoutingKey, boolean ensureBasicConfig, String exchangeType, String routingKey) {
        super(exchange, queue, threshold, prefetchCount, poisonQueueEnabled, purgeOnConnect, dynamicQueueCreation, poisonPrefix, dynamicQueueRoutingKey, ensureBasicConfig, exchangeType, routingKey);
        this.callback = callback;
    }

    public AMQPConsumerCallback getCallback() {
        return callback;
    }
}
