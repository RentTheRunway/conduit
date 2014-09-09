package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.AMQPConsumerCallback;

public class AMQPSyncConsumerBuilder extends AMQPConsumerBuilder<AMQPTransport
                                                               , AMQPListenProperties
                                                               , AMQPSyncConsumerBuilder> {
    private AMQPConsumerCallback callback;

    public static AMQPSyncConsumerBuilder builder() {
        return new AMQPSyncConsumerBuilder();
    }

    private AMQPSyncConsumerBuilder() {
    }

    public AMQPSyncConsumerBuilder callback(AMQPConsumerCallback callback) {
        this.callback = callback;
        return this;
    }

    @Override
    protected AMQPTransport buildTransport() {
        return new AMQPTransport(getHost(), getPort());
    }

    @Override
    protected AMQPListenProperties buildListenProperties() {
        return new AMQPListenProperties(callback,
                getExchange(),
                getQueue(),
                getRetryThreshold(),
                getPrefetchCount(),
                isPoisonQueueEnabled(),
                isPurgeOnConnect(),
                isDynamicQueueCreation(),
                getPoisonPrefix(),
                getDynamicQueueRoutingKey());
    }

    @Override
    protected AMQPListenContext buildListenContext(AMQPTransport transport
                                                 , AMQPConnectionProperties connectionProperties
                                                 , AMQPListenProperties listenProperties) {
        return new AMQPListenContext(transport, connectionProperties, listenProperties);
    }
}
