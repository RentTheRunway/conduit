package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;
import io.rtr.conduit.amqp.transport.TransportListenProperties;

public class AMQPAsyncTransport extends AMQPTransport {
    public AMQPAsyncTransport(String host, int port) {
        super(host, port);
    }

    @Override
    protected AMQPQueueConsumer getConsumer(Object callback, AMQPCommonListenProperties commonListenProperties, String poisonPrefix){
        return new AMQPAsyncQueueConsumer(
                getChannel(),
                (AMQPAsyncConsumerCallback) callback,
                commonListenProperties.getThreshold(),
                poisonPrefix,
                commonListenProperties.isPoisonQueueEnabled()
        );
    }

    @Override
    protected AMQPCommonListenProperties getCommonListenProperties(TransportListenProperties properties) {
        return (AMQPCommonListenProperties) properties;
    }

    @Override
    protected Object getConsumerCallback(TransportListenProperties properties) {
        return ((AMQPAsyncListenProperties)properties).getCallback();
    }
}
