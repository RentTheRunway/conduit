package io.rtr.conduit.amqp.impl;

import com.rabbitmq.client.MetricsCollector;

import io.rtr.conduit.amqp.AMQPAsyncConsumerCallback;
import io.rtr.conduit.amqp.transport.TransportListenProperties;

public class AMQPAsyncTransport extends AMQPTransport {
    public AMQPAsyncTransport(
            final boolean ssl,
            final String host,
            final int port,
            final MetricsCollector metricsCollector) {
        super(ssl, host, port, metricsCollector);
    }

    public AMQPAsyncTransport(final AMQPConnection sharedConnection) {
        super(sharedConnection);
    }

    @Override
    protected AMQPQueueConsumer getConsumer(
            final Object callback,
            final AMQPCommonListenProperties commonListenProperties,
            final String poisonPrefix) {
        return new AMQPAsyncQueueConsumer(
                this.getChannel(),
                (AMQPAsyncConsumerCallback) callback,
                commonListenProperties.getThreshold(),
                poisonPrefix,
                commonListenProperties.isPoisonQueueEnabled());
    }

    @Override
    protected AMQPCommonListenProperties getCommonListenProperties(
            final TransportListenProperties properties) {
        return (AMQPCommonListenProperties) properties;
    }

    @Override
    protected Object getConsumerCallback(final TransportListenProperties properties) {
        return ((AMQPAsyncListenProperties) properties).getCallback();
    }
}
