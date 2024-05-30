package io.rtr.conduit.amqp.impl;

import io.rtr.conduit.amqp.transport.Transport;
import io.rtr.conduit.amqp.transport.TransportConnectionProperties;
import io.rtr.conduit.amqp.transport.TransportPublishContext;
import io.rtr.conduit.amqp.transport.TransportPublishProperties;

public class AMQPPublishContext implements TransportPublishContext {
    private AMQPTransport transport;
    private AMQPConnectionProperties connectionProperties;
    private AMQPPublishProperties publishProperties;

    AMQPPublishContext(
            AMQPTransport transport,
            AMQPConnectionProperties connectionProperties,
            AMQPPublishProperties publishProperties) {
        this.transport = transport;
        this.connectionProperties = connectionProperties;
        this.publishProperties = publishProperties;
    }

    @Override
    public Transport getTransport() {
        return transport;
    }

    @Override
    public TransportConnectionProperties getConnectionProperties() {
        return connectionProperties;
    }

    @Override
    public TransportPublishProperties getPublishProperties() {
        return publishProperties;
    }
}
