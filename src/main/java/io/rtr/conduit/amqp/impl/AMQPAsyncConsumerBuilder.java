package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;

public class AMQPAsyncConsumerBuilder extends AMQPConsumerBuilder<AMQPAsyncTransport
                                                                , AMQPAsyncListenProperties
                                                                , AMQPAsyncConsumerBuilder> {
    private AMQPAsyncConsumerCallback callback;

    public static AMQPAsyncConsumerBuilder builder() {
        return new AMQPAsyncConsumerBuilder();
    }

    private AMQPAsyncConsumerBuilder() {
        super.prefetchCount(100);
    }

    public AMQPAsyncConsumerBuilder callback(AMQPAsyncConsumerCallback callback) {
        this.callback = callback;
        return this;
    }

    @Override
    protected AMQPAsyncTransport buildTransport() {
        return new AMQPAsyncTransport(getHost(), getPort());
    }

    @Override
    protected AMQPAsyncListenProperties buildListenProperties() {
        return new AMQPAsyncListenProperties(callback,
                getExchange(),
                getQueue(),
                getRetryThreshold(),
                getPrefetchCount(),
                isPoisonQueueEnabled(),
                shouldPurgeOnConnect(),
                isDynamicQueueCreation(),
                getPoisonPrefix(),
                getDynamicQueueRoutingKey(),
                isEnsureBasicConfig(),
                getExchangeType(),
                getRoutingKey());
    }

    @Override
    protected AMQPListenContext buildListenContext(AMQPAsyncTransport transport
                                                 , AMQPConnectionProperties connectionProperties
                                                 , AMQPAsyncListenProperties amqpAsyncListenProperties) {
        return new AMQPListenContext(transport, connectionProperties, amqpAsyncListenProperties);
    }
}
